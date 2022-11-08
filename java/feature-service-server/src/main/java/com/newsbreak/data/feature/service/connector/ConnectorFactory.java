package com.newsbreak.data.feature.service.connector;

import com.newsbreak.data.feature.service.connector.utils.ConnectorHelper;
import com.newsbreak.data.feature.service.controller.FeatureServiceController;
import com.newsbreak.data.feature.service.service.meta.FeatureView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.connector
 * @className: ConnectorFactory
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/19 14:55
 * @version: 1.0
 */
public class ConnectorFactory {

    private static Logger LOGGER = LoggerFactory.getLogger(ConnectorFactory.class);

    public static Connector create(FeatureView featureView) {
        if (featureView == null) {
            LOGGER.error("Empty feature view.");
            return null;
        }
        String onlineStoreType = featureView.getOnlineStoreType();
        switch (onlineStoreType) {
            case "scylla":
                return new CassandraConnector(featureView);
        }
        return null;
    }
}
