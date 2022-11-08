package com.newsbreak.data.feast.materialization.spark.utils;

import com.google.protobuf.ByteString;
import feast.proto.types.ValueProto;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.utils
 * @className: ValueUtils
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/4 14:34
 * @version: 1.0
 */
public class ValueUtils {

    public static ValueProto.Value generateValueProto(Object val, ValueProto.ValueType.Enum valueType) {
        ValueProto.Value.Builder builder = ValueProto.Value.newBuilder();
        if (val == null) {
            return builder.build();
        }
        switch (valueType) {
            case BYTES:
                return builder.setBytesVal((ByteString) val).build();
            case STRING:
                return builder.setStringVal(String.valueOf(val)).build();
            case INT32:
                return builder.setInt32Val((Integer) val).build();
            case INT64:
                return builder.setInt64Val((Long) val).build();
            case DOUBLE:
                return builder.setDoubleVal((Double) val).build();
            case FLOAT:
                return builder.setFloatVal((Float) val).build();
            case BOOL:
                return builder.setBoolVal((Boolean) val).build();
            case UNIX_TIMESTAMP:
                return builder.setUnixTimestampVal((Long) val).build();
            case STRING_LIST:
                return builder.setStringListVal((ValueProto.StringList) val).build();
            case INT32_LIST:
                return builder.setInt32ListVal((ValueProto.Int32List) val).build();
            case INT64_LIST:
                return builder.setInt64ListVal((ValueProto.Int64List) val).build();
            case DOUBLE_LIST:
                return builder.setDoubleListVal((ValueProto.DoubleList) val).build();
            case FLOAT_LIST:
                return builder.setFloatListVal((ValueProto.FloatList) val).build();
            case BOOL_LIST:
                return builder.setBoolListVal((ValueProto.BoolList) val).build();
            case UNIX_TIMESTAMP_LIST:
                return builder.setUnixTimestampListVal((ValueProto.Int64List) val).build();
            default:
                throw new RuntimeException("data type not supported");
        }
    }
}
