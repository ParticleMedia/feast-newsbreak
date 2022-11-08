package com.newsbreak.data.feature.service.connector;

//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.newsbreak.data.feature.service.FeatureValue;
import com.newsbreak.data.feature.service.service.meta.FeatureMetaService;
import com.newsbreak.data.feature.service.service.meta.FeatureView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @projectName: feature-service
 * @package: com.newsbreak.data.feature.service.connector
 * @className: CassandraConnector
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/9/20 11:37
 * @version: 1.0
 */
public class CassandraConnector implements Connector {

    private String host;

    private int port = 9042;

    private String keyspace = "default";

    private String table;

    private CqlSession cqlSession;

    private String featureViewName;

    private Logger LOGGER = LoggerFactory.getLogger(CassandraConnector.class);

    // TODO use prepared statement

    public CassandraConnector(FeatureView featureView) {
        this.host = featureView.getHost();
        this.port = featureView.getPort();
        this.keyspace = featureView.getKeyspace();
        this.table = featureView.getTable();
        this.featureViewName = featureView.getFeatureViewName();

        cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter("us-west-2")
                .build();
        LOGGER.info("Created cassandra connector: {}", featureViewName);
    }

    @Override
    public Map<String, FeatureValue> getFeatures(Map<String, String> key, List<String> featureNames) {

        if (key == null || key.size() == 0) {
            return new HashMap<>();
        }
        if (featureNames == null || featureNames.size() == 0) {
            return new HashMap<>();
        }

        FeatureView featureView = FeatureMetaService.getFeatureView(featureViewName);
        if (featureView == null) {
            // TODO
            LOGGER.error("Meta info of feature view not found: {}", featureViewName);
            return new HashMap<>();
        }
        Map<String, String> featureMap = featureView.getFeatureMap();

        StringBuffer buffer = new StringBuffer();
        buffer.append("select ");
        boolean isFirst = true;
        for (String featureName : featureNames) {
            if (!featureMap.containsKey(featureName)) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(featureName);
        }
        if (isFirst) {
            // TODO
            LOGGER.error("No features match.");
            return new HashMap<>();
        }
        buffer.append(" from ").append(keyspace).append(".").append(table).append(" where ");

        isFirst = true;
        for (Map.Entry<String, String> entry : key.entrySet()) {
            if (!featureMap.containsKey(entry.getKey())) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                buffer.append(" and ");
            }
            buffer.append(entry.getKey()).append(" = ");
            if (featureMap.get(entry.getKey()).equals("bigint")) {
                buffer.append(entry.getValue());
            } else {
                buffer.append("'").append(entry.getValue()).append("'");
            }
        }

        if (isFirst) {
            // TODO
            LOGGER.error("No entity keys match.");
            return new HashMap<>();
        }

        String cql = buffer.toString();
//        System.out.println(cql);
        LOGGER.debug("Execute sql: {}", cql);
        ResultSet resultSet = cqlSession.execute(cql);
        Iterator<Row> iter = resultSet.iterator();
        Map<String, FeatureValue> result = new HashMap<>();
        while (iter.hasNext()) {
            Row row = iter.next();
            for (int i = 0; i < featureNames.size(); i++) {
                FeatureValue featureValue;
                String dataType = featureMap.get(featureNames.get(i)).trim().toLowerCase();
                switch (dataType) {
                    case "double":
                        featureValue = FeatureValue.denseFeature(row.getDouble(i));
                        break;
                    case "list<string>":
                        featureValue = FeatureValue.sparseFeature(row.getList(i, String.class));
                        break;
                    case "list<double>":
                        featureValue = FeatureValue.embeddingFeature(row.getList(i, Double.class));
                        break;
                    default:
                        LOGGER.error("Unsupported data type {}", dataType);
                        featureValue = null;
                        break;
                }
                result.put(String.format("%s:%s", featureViewName, featureNames.get(i)), featureValue);
            }
            break;
        }
        return result;
    }

    @Override
    public Map<String, Map<String, FeatureValue>> batchGetFeatures(String keyName, List<String> keys, List<String> featureNames) {
        if (keys == null || keys.size() == 0) {
            return new HashMap<>();
        }
        if (featureNames == null || featureNames.size() == 0) {
            return new HashMap<>();
        }

        FeatureView featureView = FeatureMetaService.getFeatureView(featureViewName);
        if (featureView == null) {
            // TODO
            LOGGER.error("Meta info of feature view not found: {}", featureViewName);
            return new HashMap<>();
        }
        Map<String, String> featureMap = featureView.getFeatureMap();

        StringBuffer buffer = new StringBuffer();
        buffer.append("select ").append(keyName);
        boolean isFirst = true;
        for (String featureName : featureNames) {
            if (!featureMap.containsKey(featureName)) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            }
            buffer.append(", ").append(featureName);
        }
        if (isFirst) {
            // TODO
            LOGGER.error("No features match.");
            return new HashMap<>();
        }
        buffer.append(" from ").append(keyspace).append(".").append(table);

        if (!featureMap.containsKey(keyName)) {
            // TODO
            LOGGER.error("No entity keys match.");
            return new HashMap<>();
        }
        buffer.append(" where ").append(keyName).append(" in (");

        isFirst = true;
        for (String key : keys) {
            if (isFirst) {
                isFirst = false;
            } else {
                buffer.append(", ");
            }
            switch (featureMap.get(keyName)) {
                case "bigint":
                    buffer.append(key);
                    break;
                default:
                    buffer.append("'").append(key).append("'");
                    break;
            }
        }
        buffer.append(")");

        String cql = buffer.toString();
//        System.out.println(cql);
        LOGGER.debug("Execute sql: {}", cql);
        ResultSet resultSet = cqlSession.execute(cql);
        Iterator<Row> iter = resultSet.iterator();
        Map<String, Map<String, FeatureValue>> result = new HashMap<>();
        while (iter.hasNext()) {
            Row row = iter.next();
            String key = null;
            Map<String, FeatureValue> singleRowResult = new HashMap<>();
            for (int i = 0; i < featureNames.size() + 1; i++) {
                if (i == 0) {
                    key = row.getObject(i).toString();
                    continue;
                }
                FeatureValue featureValue;
                String dataType = featureMap.get(featureNames.get(i - 1)).trim().toLowerCase();
                switch (dataType) {
                    case "double":
                        featureValue = FeatureValue.denseFeature(row.getDouble(i));
                        break;
                    case "list<string>":
                        featureValue = FeatureValue.sparseFeature(row.getList(i, String.class));
                        break;
                    case "list<double>":
                        featureValue = FeatureValue.embeddingFeature(row.getList(i, Double.class));
                        break;
                    default:
                        LOGGER.error("Unsupported data type {}", dataType);
                        featureValue = null;
                        break;
                }
                singleRowResult.put(String.format("%s:%s", featureViewName, featureNames.get(i - 1)), featureValue);
            }
            if (result.containsKey(key)) {
                result.get(key).putAll(singleRowResult);
            } else {
                result.put(key, singleRowResult);
            }
        }
        return result;
    }

    @Override
    public void cleanup() {
        LOGGER.info("Closing cql session for feature view {}", featureViewName);
        cqlSession.close();
    }
}
