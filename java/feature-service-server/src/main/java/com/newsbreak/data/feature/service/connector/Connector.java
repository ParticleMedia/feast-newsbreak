package com.newsbreak.data.feature.service.connector;

import com.newsbreak.data.feature.service.FeatureValue;

import java.util.List;
import java.util.Map;

/**
 * @projectName: feature-service
 * @package: com.newsbreak.data.feature.service.connector
 * @className: Connector
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/9/20 11:44
 * @version: 1.0
 */
public interface Connector {

    Map<String, FeatureValue> getFeatures(Map<String, String> key, List<String> featureNames);

    Map<String, Map<String, FeatureValue>> batchGetFeatures(String keyName, List<String> keys, List<String> featureNames);

    void cleanup();
}
