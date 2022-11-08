package com.newsbreak.data.feast.materialization.spark.store.cassandra;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.service.domain
 * @className: CassandraEntity
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/1 11:16
 * @version: 1.0
 */
public class CassandraEntity {

    private String entity_key;

    private String feature_name;

    private byte[] value;

    private Timestamp event_ts;

    private Timestamp created_ts;

    public String getEntity_key() {
        return entity_key;
    }

    public void setEntity_key(String entity_key) {
        this.entity_key = entity_key;
    }

    public String getFeature_name() {
        return feature_name;
    }

    public void setFeature_name(String feature_name) {
        this.feature_name = feature_name;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Timestamp getEvent_ts() {
        return event_ts;
    }

    public void setEvent_ts(Timestamp event_ts) {
        this.event_ts = event_ts;
    }

    public Timestamp getCreated_ts() {
        return created_ts;
    }

    public void setCreated_ts(Timestamp created_ts) {
        this.created_ts = created_ts;
    }
}