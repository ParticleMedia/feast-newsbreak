namespace java com.newsbreak.data.feature.service


struct FeatureRequest {
    1: required list<string> featureNames;
    2: required map<string, string> entityIds;
}

struct BatchFeatureRequest {
    1: required list<string> featureNames;
    2: required string entityName;
    3: required list<string> entityIds;
}

union FeatureValue {
    1: double denseFeature
    2: list<string> sparseFeature
    3: list<double> embeddingFeature
}

union FeatureResponse {
    1: map<string, FeatureValue> featureValues
    2: map<string, map<string, FeatureValue>> batchFeatureValues
}

service FeatureService {
    FeatureResponse getFeatures(1: FeatureRequest request)
    FeatureResponse batchGetFeatures(1: BatchFeatureRequest request)
}