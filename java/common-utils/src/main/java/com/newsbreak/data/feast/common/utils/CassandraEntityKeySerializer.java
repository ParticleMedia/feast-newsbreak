package com.newsbreak.data.feast.common.utils;

import feast.proto.types.EntityKeyProto;
import org.apache.commons.codec.binary.Hex;

import java.util.List;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.common.utils
 * @className: CassandraEntityKeySerializer
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/2 14:56
 * @version: 1.0
 */
public class CassandraEntityKeySerializer {

    private static final int ENTITY_KEY_SERIALIZATION_VERSION = 2;

    public static String serialize(EntityKeyProto.EntityKey entityKey) {
        List<Byte> buffer = EntityKeySerializerUtils.serialize(entityKey, ENTITY_KEY_SERIALIZATION_VERSION);

        final byte[] bytes = new byte[buffer.size()];
        for (int i = 0; i < buffer.size(); i++) {
            bytes[i] = buffer.get(i);
        }

        return Hex.encodeHexString(bytes);
    }
}
