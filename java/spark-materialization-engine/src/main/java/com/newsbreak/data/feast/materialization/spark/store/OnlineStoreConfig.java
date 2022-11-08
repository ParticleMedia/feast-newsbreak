package com.newsbreak.data.feast.materialization.spark.store;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: OnlineStoreConfig
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/1 17:07
 * @version: 1.0
 */
public class OnlineStoreConfig {

    private String onlineStoreType;

    public String getOnlineStoreType() {
        return onlineStoreType;
    }

    public void setOnlineStoreType(String onlineStoreType) {
        this.onlineStoreType = onlineStoreType;
    }
}
