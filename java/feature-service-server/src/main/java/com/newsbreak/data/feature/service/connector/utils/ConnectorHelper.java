package com.newsbreak.data.feature.service.connector.utils;

import com.newsbreak.data.feature.service.connector.Connector;
import com.newsbreak.data.feature.service.connector.ConnectorFactory;
import com.newsbreak.data.feature.service.service.meta.FeatureMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.connector.utils
 * @className: ConnectorHelper
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/19 11:04
 * @version: 1.0
 */
public class ConnectorHelper {

    private static ConcurrentHashMap<String, Connector> featureViewConnectorMap = new ConcurrentHashMap<>();
    private static Logger LOGGER = LoggerFactory.getLogger(ConnectorHelper.class);

    public static Connector getConnector(String featureViewName) {
        if (!featureViewConnectorMap.containsKey(featureViewName)) {
            synchronized (ConnectorHelper.class) {
                if (!featureViewConnectorMap.containsKey(featureViewName)) {
                    LOGGER.info("Add connector: {}", featureViewName);
                    featureViewConnectorMap.put(featureViewName, ConnectorFactory.create(FeatureMetaService.getFeatureView(featureViewName)));
                }
            }
        }
        return featureViewConnectorMap.get(featureViewName);
    }

    public static void removeConnector(String featureViewName) {
        if (featureViewConnectorMap.containsKey(featureViewName)) {
            synchronized (ConnectorHelper.class) {
                if (featureViewConnectorMap.containsKey(featureViewName)) {
                    LOGGER.info("Remove connector: {}", featureViewName);
                    Connector connector = featureViewConnectorMap.remove(featureViewName);
                    connector.cleanup();
                }
            }
        }
    }
}
