package com.nb.data;

import com.newsbreak.data.feast.common.utils.CassandraEntityKeySerializer;
import feast.proto.types.EntityKeyProto;
import feast.proto.types.ValueProto;
import org.junit.Test;

/**
 * @projectName: feast-newsbreak
 * @package: com.nb.data
 * @className: CassandraEntityKeySerializer
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/2 15:18
 * @version: 1.0
 */
public class CassandraEntityKeySerializerTest {

    @Test
    public void testCassandraEntityKeySerializer() {
        long driverId = 1005;
        EntityKeyProto.EntityKey entityKey = EntityKeyProto.EntityKey.newBuilder()
                .addJoinKeys("driver_id")
                .addEntityValues(ValueProto.Value.newBuilder().setInt64Val(driverId).build())
                .build();
        assert("020000006472697665725f69640400000008000000ed03000000000000".equals(CassandraEntityKeySerializer.serialize(entityKey)));
    }
}
