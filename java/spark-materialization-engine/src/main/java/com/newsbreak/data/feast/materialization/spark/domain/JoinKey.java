package com.newsbreak.data.feast.materialization.spark.domain;

import feast.proto.types.ValueProto;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: JoinKey
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/2 15:34
 * @version: 1.0
 */
public class JoinKey {

    private String name;

    private Object value;

    private ValueProto.ValueType.Enum valueType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public ValueProto.ValueType.Enum getValueType() {
        return valueType;
    }

    public void setValueType(ValueProto.ValueType.Enum valueType) {
        this.valueType = valueType;
    }
}
