package com.newsbreak.data.feast.materialization.spark.config;

import com.newsbreak.data.feast.materialization.spark.store.cassandra.CassandraOnlineStoreConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: MaterializationConfig
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/1 17:06
 * @version: 1.0
 */
public class MaterializationConfig {

    private String sql;

    private String metaUris;

    private Map<String, String> fieldsMapping = new HashMap<>();

    private String featureView;

    private Map<String, String> features;

    private Map<String, String> joinKeys;

    private String timestampField;

    private Integer ttl;

    private String project;

    private CassandraOnlineStoreConfig onlineStoreConfig;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getMetaUris() {
        return metaUris;
    }

    public void setMetaUris(String metaUris) {
        this.metaUris = metaUris;
    }

    public Map<String, String> getFieldsMapping() {
        return fieldsMapping;
    }

    public void setFieldsMapping(Map<String, String> fieldsMapping) {
        this.fieldsMapping = fieldsMapping;
    }

    public String getFeatureView() {
        return featureView;
    }

    public void setFeatureView(String featureView) {
        this.featureView = featureView;
    }

    public Map<String, String> getFeatures() {
        return features;
    }

    public void setFeatures(Map<String, String> features) {
        this.features = features;
    }

    public Map<String, String> getJoinKeys() {
        return joinKeys;
    }

    public void setJoinKeys(Map<String, String> joinKeys) {
        this.joinKeys = joinKeys;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    public CassandraOnlineStoreConfig getOnlineStoreConfig() {
        return onlineStoreConfig;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public void setOnlineStoreConfig(CassandraOnlineStoreConfig onlineStoreConfig) {
        this.onlineStoreConfig = onlineStoreConfig;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }
}
