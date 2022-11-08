package com.newsbreak.data.feast.materialization.spark.config;

/**
 * @projectName: feast-newsbreak
 * @package: com.newsbreak.data.feast.materialization.spark.domain
 * @className: Config
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/11/1 17:06
 * @version: 1.0
 */
public class Config {

    private MaterializationConfig materialization;

    private SparkConfig spark;

    public MaterializationConfig getMaterialization() {
        return materialization;
    }

    public void setMaterialization(MaterializationConfig materialization) {
        this.materialization = materialization;
    }

    public SparkConfig getSpark() {
        return spark;
    }

    public void setSpark(SparkConfig spark) {
        this.spark = spark;
    }
}
