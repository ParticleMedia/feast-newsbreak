package com.nb.data;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.newsbreak.data.feature.service.service.meta.api.ApiResponse;
import com.newsbreak.data.feature.service.utils.HttpHelper;
import org.junit.Test;

import java.util.List;

/**
 * @projectName: data-feature-service
 * @package: com.nb.data
 * @className: MetaTest
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/18 14:58
 * @version: 1.0
 */
public class MetaTest {

    @Test
    public void testMetaStr() {

        String URL_HOSTNAME = "http://feast-api.k8s.nb-stage.com";
        String METHOD_FEATURE_VIEW_LIST = "/api/v1/list_feature_view";
        String METHOD_GET_ENTITIES_BY_VIEW = "/api/v1/get_entities_by_view";

        String featureViewListStr = HttpHelper.getJsonStrFromUrl(URL_HOSTNAME + METHOD_FEATURE_VIEW_LIST);
        System.out.println(featureViewListStr);

        TypeToken<ApiResponse<List<String>>> stringListToken = new TypeToken<ApiResponse<List<String>>>() {};
//        ApiResponse<List<String>> featureViewListResult = new ApiResponse<>();
        ApiResponse<List<String>> featureViewListResponse = new Gson().fromJson(featureViewListStr, stringListToken.getType());
        for (String featureViewName : featureViewListResponse.getData()) {
            System.out.println(featureViewName);
//            String entityStr = HttpHelper.getJsonStrFromUrl(URL_HOSTNAME + METHOD_GET_ENTITIES_BY_VIEW + "?feature_name=" + featureViewName);
//            FeatureView featureView = new FeatureView();
        }
    }



}
