package com.nb.data;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.newsbreak.data.feast.common.utils.CassandraEntityKeySerializer;
import feast.proto.types.EntityKeyProto;
import feast.proto.types.ValueProto;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * @projectName: feast-newsbreak
 * @package: com.nb.data
 * @className: CassandraTest
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/2 11:00
 * @version: 1.0
 */
public class CassandraTest {

    @Test
    public void testFetchData() throws InvalidProtocolBufferException {
        CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("shdata.scylla.nb.com", 9042))
                .withLocalDatacenter("us-west-2")
                .build();

        PreparedStatement preparedStatement = cqlSession.prepare("select * from feast_dev.rapid_panther_driver_hourly_stats_fresh where entity_key = ?");
        ResultSet resultSet = cqlSession.execute(preparedStatement.bind("020000006472697665725f69640400000008000000ed03000000000000"));
//        PreparedStatement preparedStatement = cqlSession.prepare("select * from feast_dev.feast_24_nova_user_base_features_daily_view where entity_key = ?");
//        ResultSet resultSet = cqlSession.execute(preparedStatement.bind("02000000757365725f69640200000009000000313934313937343638"));
        Iterator<Row> iter = resultSet.iterator();
        while (iter.hasNext()) {
            Row row = iter.next();
            System.out.println("--- new row ---");
            System.out.println("entity_key: " + row.getString("entity_key"));
            System.out.println("feature_name: " + row.getString("feature_name"));
            System.out.println("created_ts: " + row.get("created_ts", TypeCodecs.TIMESTAMP));
            System.out.println("event_ts: " + row.get("event_ts", TypeCodecs.TIMESTAMP));
            System.out.println("value: " + ValueProto.Value.parseFrom(row.get("value", TypeCodecs.BLOB)).getStringVal());
            System.out.println("---------------");
        }
    }

    @Test
    public void testGenerateEntityKey() {
        long driverId = 1005;

        EntityKeyProto.EntityKey entityKey = EntityKeyProto.EntityKey.newBuilder()
                .addJoinKeys("driver_id")
                .addEntityValues(ValueProto.Value.newBuilder().setInt64Val(driverId).build())
                .build();
//        byte[] bytes = new EntityKeySerializer(2).serialize(entityKey);
        System.out.println(CassandraEntityKeySerializer.serialize(entityKey));
    }
}
