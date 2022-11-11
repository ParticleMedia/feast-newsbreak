package com.newsbreak.data.feature.service.service.meta;

import com.amazonaws.services.s3.AmazonS3;
import com.newsbreak.data.feature.service.FeatureRequest;
import feast.proto.core.FeatureViewProto;
import feast.proto.serving.ServingAPIProto;
import feast.serving.registry.RegistryRepository;
import feast.serving.registry.S3RegistryFile;

/**
 * Created by ricky on 2022/11/11.
 */
public class UnifiedFeatureMetaServiceImpl implements UnifiedFeatureMetaService {
    private static FeatureMetaService internalFeatureMetaRepo;
    private static RegistryRepository feastFeatureMetaRepo;
    private static final int FEATURE_META_REPO_REFRESH_SECONDS = 500;

    public UnifiedFeatureMetaServiceImpl(FeatureMetaService featureMetaService, RegistryRepository registryRepository){
        this.internalFeatureMetaRepo = featureMetaService;
        this.feastFeatureMetaRepo = registryRepository;
    }


    @Override
    public boolean isInternalFormatFeature(String featureViewName) throws FeatureMetaException {
        ServingAPIProto.FeatureReferenceV2 featureReference = ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName(featureViewName)
                .build();
        FeatureView internalFeatureView = this.internalFeatureMetaRepo.getFeatureView(featureReference);
        FeatureViewProto.FeatureViewSpec feastFeatureView = this.feastFeatureMetaRepo.getFeatureViewSpec(featureReference);

        if (internalFeatureView == null && feastFeatureView == null) {
            throw new FeatureMetaException(String.format("Cannot allocate this feature view $s", featureViewName));
        } else if (internalFeatureView != null && feastFeatureView != null) {
            throw new FeatureMetaException(String.format("FeatureView $s exists in both internal meta and feast meta.", featureViewName));
        } else {
            return (internalFeatureView != null);
        }
    }

    @Override
    public FeatureMetaService getInternalFeatureMetaRepo() {
        return internalFeatureMetaRepo;
    }

    @Override
    public RegistryRepository getFeastFeatureMetaRepo() {
        return feastFeatureMetaRepo;
    }
}
