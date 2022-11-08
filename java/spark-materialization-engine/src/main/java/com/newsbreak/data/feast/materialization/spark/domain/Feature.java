package com.newsbreak.data.feast.materialization.spark.domain;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: Feature
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/3 10:30
 * @version: 1.0
 */
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
