package com.newsbreak.data.feature.service.module;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.cloud.storage.Storage;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.newsbreak.data.feature.service.service.meta.FeatureMetaService;
import com.newsbreak.data.feature.service.service.meta.UnifiedFeatureMetaService;
import com.newsbreak.data.feature.service.service.meta.UnifiedFeatureMetaServiceImpl;
import com.twitter.inject.TwitterModule;
import feast.serving.registry.*;
import feast.serving.service.config.ApplicationProperties;

import java.net.URI;
import java.util.Optional;

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

    @Singleton
    @Provides
    RegistryRepository registryRepository(
            RegistryFile registryFile) {
        return new RegistryRepository(
                registryFile, 500);
    }

    @Provides
    public AmazonS3 awsStorage() {
        return AmazonS3ClientBuilder.standard()
                .withRegion("us-west-2")
                .build();
    }

    @Provides
    RegistryFile registryFile(
            Provider<AmazonS3> amazonS3Provider) {

        //TODO change to prod
        String registryPath = "s3://newsbreak-feast/registry/staging/feature_repo.db";
        Optional<String> scheme = Optional.ofNullable(URI.create(registryPath).getScheme());

        switch (scheme.orElse("")) {
            case "s3":
                return new S3RegistryFile(amazonS3Provider.get(), registryPath);
            case "":
            case "file":
                return new LocalRegistryFile(registryPath);
            default:
                throw new RuntimeException(
                        String.format("Registry storage %s is unsupported", scheme.get()));
        }
    }

    @Singleton
    @Provides
    private UnifiedFeatureMetaService unifiedFeatureMetaService(
            FeatureMetaService featureMetaService,
            RegistryRepository registryRepository
    ) {
        return new UnifiedFeatureMetaServiceImpl(featureMetaService, registryRepository);
    }
}
