package com.newsbreak.data.feature.service.module;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.newsbreak.data.feature.service.service.meta.FeatureMetaService;
import com.twitter.inject.TwitterModule;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.module
 * @className: FeatureMetaModule
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/14 10:54
 * @version: 1.0
 */
public class FeatureMetaModule extends TwitterModule {

    @Singleton
    @Provides
    private FeatureMetaService featureMetaService() {
        FeatureMetaService featureMetaService = new FeatureMetaService();
        featureMetaService.startAsync();
        return featureMetaService;
    }

}
