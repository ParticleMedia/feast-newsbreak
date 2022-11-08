package com.newsbreak.data.feature.service.service.meta.api;

import java.util.List;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.service.meta.api
 * @className: ApiResponseEntity
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/20 15:36
 * @version: 1.0
 */
public class ApiResponseEntity {

    private String host;

    private Integer port;

    private String keyspace;

    private String table;

    private String onlineStoreType;

    private List<Feature> schema;

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

    public String getOnlineStoreType() {
        return onlineStoreType;
    }

    public void setOnlineStoreType(String onlineStoreType) {
        this.onlineStoreType = onlineStoreType;
    }

    public List<Feature> getSchema() {
        return schema;
    }

    public void setSchema(List<Feature> schema) {
        this.schema = schema;
    }

    public class Feature {

        private String name;

        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
