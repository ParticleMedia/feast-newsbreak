package com.newsbreak.data.feast.materialization.spark;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.newsbreak.data.feast.common.utils.CassandraEntityKeySerializer;
import com.newsbreak.data.feast.materialization.spark.store.cassandra.CassandraEntity;
import com.newsbreak.data.feast.materialization.spark.config.Config;
import com.newsbreak.data.feast.materialization.spark.domain.JoinKey;
import com.newsbreak.data.feast.materialization.spark.utils.ValueUtils;
import feast.proto.types.EntityKeyProto;
import feast.proto.types.ValueProto;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark
 * @className: Main
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/1 10:31
 * @version: 1.0
 */
public class Main {

    public static void main(String[] args) throws ParseException, IOException {

        Options options = new Options();
        options.addOption("f", "file_name", true, "file name");
        options.addOption("c", "config", true, "config string");

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(options, args);

        // read config
        Config config;
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        if (cmdLine.hasOption("config")) {
            String configStr = cmdLine.getOptionValue("config");
            config = gson.fromJson(configStr, Config.class);
        } else {
            throw new RuntimeException("Missing file_name or config.");
        }

        String tableName = String.format("%s_%s",
                config.getMaterialization().getProject(),
                config.getMaterialization().getFeatureView());

        // init spark conf and session
        SparkConf sparkConf = new SparkConf();
        if (config.getSpark() != null) {
            for (Map.Entry<String, String> entry : config.getSpark().getConfig().entrySet()) {
                sparkConf.set(entry.getKey(), entry.getValue());
            }
        }

        SparkSession.Builder builder = SparkSession.builder();
        builder.config("spark.sql.catalogImplementation", "hive")
                .config("spark.shuffle.useOldFetchProtocol", "true")
                .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .enableHiveSupport()
                .config(sparkConf)
                .config("spark.cassandra.connection.host", String.join(",", config.getMaterialization().getOnlineStoreConfig().getHost()))
                .config("spark.cassandra.connection.port", config.getMaterialization().getOnlineStoreConfig().getPort());

        if (!StringUtils.isBlank(config.getMaterialization().getMetaUris())) {
            builder.config("hive.metastore.uris", config.getMaterialization().getMetaUris());
        }

        if (!StringUtils.isBlank(config.getSpark().getAppName())) {
            builder.appName(config.getSpark().getAppName());
        }

        SparkSession session = builder.getOrCreate();

        System.out.println(config.getMaterialization().getSql());

        Dataset<Row> dataSet = session.sql(config.getMaterialization().getSql());
        dataSet.show(10);

        // field mapping
        for (Map.Entry<String, String> fieldMappingEntry : config.getMaterialization().getFieldsMapping().entrySet()) {
            dataSet = dataSet.withColumnRenamed(fieldMappingEntry.getKey(), fieldMappingEntry.getValue());
        }

        final Map<String, String> joinKeys = config.getMaterialization().getJoinKeys();
        final Map<String, String> features = config.getMaterialization().getFeatures();
        int joinKeyNum = joinKeys.size();
        int featureNum = features.size();

        // init select columns
        final String[] selectColumnNames;
        final String timestampField = config.getMaterialization().getTimestampField();
        if (!StringUtils.isBlank(timestampField)) {
            selectColumnNames = new String[joinKeyNum + featureNum + 1];
            // add timestamp column to select columns
            selectColumnNames[joinKeyNum + featureNum] = config.getMaterialization().getTimestampField();
        } else {
            selectColumnNames = new String[joinKeyNum + featureNum];
        }

        // add join key columns to select columns
        int i = 0;
        for (String joinKeyName: joinKeys.keySet()) {
            selectColumnNames[i] = joinKeyName;
            i++;
        }

        // add feature columns to select columns
        for (String featureName : features.keySet()) {
            selectColumnNames[i] = featureName;
            i++;
        }

        Seq<Column> selectColumnNameSeq = JavaConverters.collectionAsScalaIterableConverter(
                Arrays.asList(selectColumnNames).stream().map(
                        name -> new Column(name)).collect(Collectors.toList())).asScala().toSeq();

        JavaRDD<CassandraEntity> rdd = dataSet.select(selectColumnNameSeq)
                .javaRDD().flatMap(new FlatMapFunction<Row, CassandraEntity>() {

                    @Override
                    public Iterator<CassandraEntity> call(Row row) throws Exception {
                        List<JoinKey> joinKeyList = new ArrayList<>();
                        int i = 0;
                        for (; i < joinKeys.size(); i++) {
                            JoinKey joinKey = new JoinKey();
                            String joinKeyName = selectColumnNames[i];
                            joinKey.setName(joinKeyName);
                            joinKey.setValue(row.get(i));
                            joinKey.setValueType(ValueProto.ValueType.Enum.valueOf(joinKeys.get(joinKeyName)));
                            joinKeyList.add(joinKey);
                        }
                        String entityKey = CassandraEntityKeySerializer.serialize(generateEntityKeyProto(joinKeyList));

                        Timestamp ts = null;
                        if (!StringUtils.isBlank(timestampField)) {
                            ts = row.getTimestamp(selectColumnNames.length - 1);
                        }

                        List<CassandraEntity> resultList = new ArrayList<>();

                        for (; i < features.size(); i++) {
                            String featureName = selectColumnNames[i];
                            CassandraEntity cassandraEntity = new CassandraEntity();
                            cassandraEntity.setEntity_key(entityKey);
                            cassandraEntity.setFeature_name(featureName);
                            cassandraEntity.setValue(ValueUtils.generateValueProto(row.get(i), ValueProto.ValueType.Enum.valueOf(features.get(featureName))).toByteArray());
                            if (ts != null) {
                                cassandraEntity.setEvent_ts(ts);
                            }
                            resultList.add(cassandraEntity);
                        }

                        return resultList.iterator();
                    }
                });

        Dataset<Row> resultDataset = session.sqlContext().applySchema(rdd, CassandraEntity.class);
        resultDataset.show(10);

        DataFrameWriter dataFrameWriter = resultDataset.write()
                .mode("append")
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", config.getMaterialization().getOnlineStoreConfig().getKeyspace())
                .option("table", tableName)
                .option("output.consistency.level", "ALL");
        if (config.getMaterialization().getTtl() != null) {
            dataFrameWriter.option("ttl", config.getMaterialization().getTtl());
        }
        dataFrameWriter.save();

        session.close();

    }

    public static EntityKeyProto.EntityKey generateEntityKeyProto(List<JoinKey> joinKeys) {
        EntityKeyProto.EntityKey.Builder builder = EntityKeyProto.EntityKey.newBuilder();
        for (JoinKey joinKey : joinKeys) {
            String name = joinKey.getName();
            ValueProto.Value value = ValueUtils.generateValueProto(joinKey.getValue(), joinKey.getValueType());
            builder.addJoinKeys(name).addEntityValues(value);
        }
        return builder.build();
    }
}
