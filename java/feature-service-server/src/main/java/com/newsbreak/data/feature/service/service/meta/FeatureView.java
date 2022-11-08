package com.newsbreak.data.feature.service.service.meta;

import com.newsbreak.data.feature.service.service.meta.api.ApiResponseEntity;

import java.util.HashMap;
import java.util.Map;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.service.meta
 * @className: FeatureView
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/20 10:41
 * @version: 1.0
 */
public class FeatureView {

    private String onlineStoreType;

    private String host;

    private Integer port;

    private String keyspace;

    private String table;

    private Map<String, String> featureMap;

    private String featureViewName;

    public String getOnlineStoreType() {
        return onlineStoreType;
    }

    public void setOnlineStoreType(String onlineStoreType) {
        this.onlineStoreType = onlineStoreType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, String> getFeatureMap() {
        return featureMap;
    }

    public void setFeatureMap(Map<String, String> featureMap) {
        this.featureMap = featureMap;
    }

    public String getFeatureViewName() {
        return featureViewName;
    }

    public void setFeatureViewName(String featureViewName) {
        this.featureViewName = featureViewName;
    }

    public FeatureView(String featureViewName, ApiResponseEntity entity) {
        this.host = entity.getHost();
        this.port = entity.getPort();
        this.keyspace = entity.getKeyspace();
        this.table = entity.getTable();
        this.onlineStoreType = entity.getOnlineStoreType();
        this.featureViewName = featureViewName;

        Map<String, String> featureMap = new HashMap<>();
        for (ApiResponseEntity.Feature feature : entity.getSchema()) {
            featureMap.put(feature.getName(), feature.getType());
        }
        this.featureMap = featureMap;
    }
}
