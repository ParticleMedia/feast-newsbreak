package com.newsbreak.data.feast.common.utils;

import com.google.protobuf.ProtocolStringList;
import feast.proto.types.EntityKeyProto;
import feast.proto.types.ValueProto;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.common.utils
 * @className: EntityKeySerializerUtils
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/2 14:59
 * @version: 1.0
 */
public class EntityKeySerializerUtils {

    public static List<Byte> serialize(EntityKeyProto.EntityKey entityKey, int entityKeySerializationVersion) {
        final ProtocolStringList joinKeys = entityKey.getJoinKeysList();
        final List<ValueProto.Value> values = entityKey.getEntityValuesList();

        assert joinKeys.size() == values.size();

        final List<Byte> buffer = new ArrayList<>();

        final List<Pair<String, ValueProto.Value>> tuples = new ArrayList<>(joinKeys.size());
        for (int i = 0; i < joinKeys.size(); i++) {
            tuples.add(Pair.of(joinKeys.get(i), values.get(i)));
        }
        tuples.sort(Comparator.comparing(Pair::getLeft));

        for (Pair<String, ValueProto.Value> pair : tuples) {
            buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.STRING.getNumber()));
            buffer.addAll(encodeString(pair.getLeft()));
        }

        for (Pair<String, ValueProto.Value> pair : tuples) {
            final ValueProto.Value val = pair.getRight();
            switch (val.getValCase()) {
                case STRING_VAL:
                    String stringVal = val.getStringVal();

                    buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.STRING.getNumber()));
                    buffer.addAll(encodeInteger(stringVal.length()));
                    buffer.addAll(encodeString(stringVal));

                    break;
                case BYTES_VAL:
                    byte[] bytes = val.getBytesVal().toByteArray();

                    buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.BYTES.getNumber()));
                    buffer.addAll(encodeInteger(bytes.length));
                    buffer.addAll(encodeBytes(bytes));

                    break;
                case INT32_VAL:
                    buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.INT32.getNumber()));
                    buffer.addAll(encodeInteger(Integer.BYTES));
                    buffer.addAll(encodeInteger(val.getInt32Val()));

                    break;
                case INT64_VAL:
                    buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.INT64.getNumber()));
          /* This is super dumb - but in https://github.com/feast-dev/feast/blob/dcae1606f53028ce5413567fb8b66f92cfef0f8e/sdk/python/feast/infra/key_encoding_utils.py#L9
          we use `struct.pack("<l", v.int64_val)` to get the bytes of an int64 val. This actually extracts only 4 bytes,
          instead of 8 bytes as you'd expect from to serialize an int64 value.
          */
                    if (entityKeySerializationVersion <= 1) {
                        buffer.addAll(encodeInteger(Integer.BYTES));
                        buffer.addAll(encodeInteger(((Long) val.getInt64Val()).intValue()));
                    } else {
                        buffer.addAll(encodeInteger(Long.BYTES));
                        buffer.addAll(encodeLong(((Long) val.getInt64Val())));
                    }

                    break;
                default:
                    throw new RuntimeException("Unable to serialize Entity Key");
            }
        }

        return buffer;
    }

    private static List<Byte> encodeBytes(byte[] toByteArray) {
        return Arrays.asList(ArrayUtils.toObject(toByteArray));
    }

    private static List<Byte> encodeInteger(Integer value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(value);

        return Arrays.asList(ArrayUtils.toObject(buffer.array()));
    }

    private static List<Byte> encodeLong(Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);

        return Arrays.asList(ArrayUtils.toObject(buffer.array()));
    }

    private static List<Byte> encodeString(String value) {
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
        return encodeBytes(stringBytes);
    }
}
