package com.newsbreak.data.feature.service.service.meta;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.newsbreak.data.feature.service.connector.utils.ConnectorHelper;
import com.newsbreak.data.feature.service.service.meta.api.ApiResponse;
import com.newsbreak.data.feature.service.service.meta.api.ApiResponseEntity;
import com.newsbreak.data.feature.service.utils.HttpHelper;
import feast.proto.serving.ServingAPIProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.service.meta
 * @className: FeatureMetaService
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/17 17:16
 * @version: 1.0
 */
public class FeatureMetaService extends AbstractScheduledService {

    private static final ConcurrentMap<String, FeatureView> featureViewMap = new ConcurrentHashMap<>();
    private static String lastMetaMD5 = null;

    private static final String URL_HOSTNAME = "http://feast-api.k8s.nb-stage.com";
    private static final String METHOD_FEATURE_VIEW_LIST = "/api/v1/list_feature_view";
    private static final String METHOD_GET_ENTITIES_BY_VIEW = "/api/v1/get_entities_by_view";

    private static final int API_RESPONSE_SUCCESS_CODE = 200;
    private static final int DEFAULT_SCHEDULE_INTERVAL_SECONDS = 300;
    private static int SCHEDULE_INTERVAL_SECONDS;

    private Logger LOGGER = LoggerFactory.getLogger(FeatureMetaService.class);

    private Gson gson = new Gson();

    @Override
    protected void runOneIteration() {
        String featureViewListStr = HttpHelper.getJsonStrFromUrl(URL_HOSTNAME + METHOD_FEATURE_VIEW_LIST);
        TypeToken<ApiResponse<List<String>>> featureViewListResponseType = new TypeToken<ApiResponse<List<String>>>() {};
        ApiResponse<List<String>> featureViewListResponse = gson.fromJson(featureViewListStr, featureViewListResponseType.getType());
        if (featureViewListResponse.getCode() != API_RESPONSE_SUCCESS_CODE) {
            LOGGER.error("Failed to get feature view list, full response: {}", featureViewListStr);
            return;
        }

        for (String featureViewName : featureViewListResponse.getData()) {
            String entityStr = HttpHelper.getJsonStrFromUrl(URL_HOSTNAME + METHOD_GET_ENTITIES_BY_VIEW + "?feature_name=" + featureViewName);
            TypeToken<ApiResponse<ApiResponseEntity>> entityResponseType = new TypeToken<ApiResponse<ApiResponseEntity>>() {};
            ApiResponse<ApiResponseEntity> entityResponse = gson.fromJson(entityStr, entityResponseType.getType());
            if (entityResponse.getCode() != API_RESPONSE_SUCCESS_CODE) {
                LOGGER.error("Failed to get entity, full response: {}", featureViewListStr);
                continue;
            }
            FeatureView featureView = new FeatureView(featureViewName, entityResponse.getData());
            featureViewMap.put(featureViewName, featureView);
        }

        removeDeprecatedFeatureView(featureViewListResponse.getData());
    }

    public FeatureMetaService(){
        this.SCHEDULE_INTERVAL_SECONDS = this.DEFAULT_SCHEDULE_INTERVAL_SECONDS;
    }

    public FeatureMetaService(int scheduleInervalSeconds){
        this.SCHEDULE_INTERVAL_SECONDS = scheduleInervalSeconds;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0,SCHEDULE_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected void startUp() {
        runOneIteration();
    }

    public static FeatureView getFeatureView(ServingAPIProto.FeatureReferenceV2 featureReference) {
        return featureViewMap.get(featureReference.getFeatureViewName());
    }

    private void removeDeprecatedFeatureView(List<String> featureViewNameList) {
        Set<String> featureViewNameSet = Sets.newHashSet(featureViewNameList);
        for (String featureViewName : featureViewMap.keySet()) {
            if (!featureViewNameSet.contains(featureViewName)) {
                LOGGER.info("Remove feature view: {}", featureViewName);
                removeFeatureView(featureViewName);
                ConnectorHelper.removeConnector(featureViewName);
            }
        }
    }

    private void removeFeatureView(String featureViewName) {
        if (featureViewMap.containsKey(featureViewName)) {
            synchronized (FeatureMetaService.class) {
                if (featureViewMap.containsKey(featureViewName)) {
                    featureViewMap.remove(featureViewName);
                }
            }
        }
    }
}
