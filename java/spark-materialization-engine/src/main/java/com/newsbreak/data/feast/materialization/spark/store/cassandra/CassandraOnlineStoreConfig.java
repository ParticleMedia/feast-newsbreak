package com.newsbreak.data.feast.materialization.spark.store.cassandra;

import com.newsbreak.data.feast.materialization.spark.store.OnlineStoreConfig;

import java.util.List;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: CassandraOnlineStoreConfig
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/3 10:35
 * @version: 1.0
 */
public class CassandraOnlineStoreConfig extends OnlineStoreConfig {

    private List<String> host;

    private Integer port = 9042;

    private String keyspace;

    public List<String> getHost() {
        return host;
    }

    public void setHost(List<String> host) {
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
}
