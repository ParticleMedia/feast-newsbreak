package com.newsbreak.data.feature.service.module;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.newsbreak.data.feature.service.controller.FeatureServiceController;
import com.twitter.inject.TwitterModule;

/**
 * @projectName: feature-service
 * @package: com.newsbreak.data.feature.service.module
 * @className: ControllerModule
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/9/22 14:54
 * @version: 1.0
 */
public class ControllerModule extends TwitterModule {

    @Singleton
    @Provides
    private FeatureServiceController featureServiceController() {
        return new FeatureServiceController();
    }

}
