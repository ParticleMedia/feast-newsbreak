package com.newsbreak.data.feature.service.service.meta;

import com.newsbreak.data.feature.service.FeatureRequest;
import com.newsbreak.data.feature.service.service.meta.FeatureMetaService;
import feast.serving.registry.RegistryRepository;

/**
 * Created by ricky on 2022/11/11.
 */
public interface UnifiedFeatureMetaService {

    boolean isInternalFormatFeature(String featureViewName) throws FeatureMetaException;
    FeatureMetaService getInternalFeatureMetaRepo();
    RegistryRepository getFeastFeatureMetaRepo();
}
