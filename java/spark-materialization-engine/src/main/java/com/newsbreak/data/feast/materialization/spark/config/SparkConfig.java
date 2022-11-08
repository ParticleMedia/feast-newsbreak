package com.newsbreak.data.feast.materialization.spark.config;

import java.util.HashMap;
import java.util.Map;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: SparkConfig
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/4 14:45
 * @version: 1.0
 */
public class SparkConfig {

    private String appName;

    private Map<String, String> config = new HashMap<>();

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}
