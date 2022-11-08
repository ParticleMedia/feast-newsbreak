package com.newsbreak.data.feature.service.controller;

import com.google.common.collect.Lists;
import com.newsbreak.data.feature.service.*;
import com.newsbreak.data.feature.service.connector.Connector;
import com.newsbreak.data.feature.service.connector.utils.ConnectorHelper;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @projectName: feature-service
 * @package: com.newsbreak.data.feature.service.controller
 * @className: FeatureServiceController
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/9/22 14:50
 * @version: 1.0
 */
public class FeatureServiceController implements FeatureService.ServiceIface {

    private Logger LOGGER = LoggerFactory.getLogger(FeatureServiceController.class);

    private ExecutorService executorService = Executors.newFixedThreadPool(200);

    // TODO completable future

    @Override
    public Future<FeatureResponse> getFeatures(FeatureRequest request) {

        // TODO
        // check features and entities

        Map<String, List<String>> featureViewToFeatureListMap = new HashMap<>();
        for (String feature : request.getFeatureNames()) {
            String[] fields = feature.split(":");
            String featureViewName = fields[0];
            String featureName = fields[1];

            List<String> requestFeatureList = featureViewToFeatureListMap.getOrDefault(featureViewName, new ArrayList<>());
            requestFeatureList.add(featureName);
            featureViewToFeatureListMap.put(featureViewName, requestFeatureList);
        }

        // TODO executor pool to get
        Map<String, FeatureValue> result = new HashMap<>();
        List<java.util.concurrent.Future<Map<String, FeatureValue>>> futures = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : featureViewToFeatureListMap.entrySet()) {
            String featureViewName = entry.getKey();
            Connector connector = ConnectorHelper.getConnector(featureViewName);
            if (connector == null) {
                // TODO
                continue;
            }
            Callable<Map<String, FeatureValue>> callable = () -> connector.getFeatures(request.getEntityIds(), Lists.newArrayList(entry.getValue()));
            java.util.concurrent.Future<Map<String, FeatureValue>> future = executorService.submit(callable);
            futures.add(future);
//            result.putAll(connector.getFeatures(request.getEntityIds(), Lists.newArrayList(entry.getValue())));
        }

        for (java.util.concurrent.Future<Map<String, FeatureValue>> future : futures) {
            try {
                result.putAll(future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return Future.exception(e);
//                return Future.value(null);
            }
        }

        FeatureResponse response = FeatureResponse.featureValues(result);
        return Future.value(response);
    }

    @Override
    public Future<FeatureResponse> batchGetFeatures(BatchFeatureRequest request) {

        // TODO
        // check features and entities

        // TODO entity name cannot be same in different feature view
        Map<String, List<String>> featureViewToFeatureListMap = new HashMap<>();
        for (String feature : request.getFeatureNames()) {
            String[] fields = feature.split(":");
            String featureViewName = fields[0];
            String featureName = fields[1];

            List<String> requestFeatureList = featureViewToFeatureListMap.getOrDefault(featureViewName, new ArrayList<>());
            requestFeatureList.add(featureName);
            featureViewToFeatureListMap.put(featureViewName, requestFeatureList);
        }

        Map<String, Map<String, FeatureValue>> result = new HashMap<>();
        List<java.util.concurrent.Future<Map<String, Map<String, FeatureValue>>>> futures = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : featureViewToFeatureListMap.entrySet()) {
            String featureViewName = entry.getKey();
            Connector connector = ConnectorHelper.getConnector(featureViewName);
            if (connector == null) {
                // TODO
                continue;
            }

//            Map<String, Map<String, FeatureValue>> queryResult
//                    = connector.batchGetFeatures(request.getEntityName(), request.getEntityIds(), Lists.newArrayList(entry.getValue()));

            Callable<Map<String, Map<String, FeatureValue>>> callable = () -> connector.batchGetFeatures(request.getEntityName(), request.getEntityIds(), Lists.newArrayList(entry.getValue()));
            java.util.concurrent.Future<Map<String, Map<String, FeatureValue>>> future = executorService.submit(callable);
            futures.add(future);
        }

        for (java.util.concurrent.Future<Map<String, Map<String, FeatureValue>>> future : futures) {
            Map<String, Map<String, FeatureValue>> queryResult;
            try {
                queryResult = future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return Future.exception(e);
//                return Future.value(null);
            }

            for (Map.Entry<String, Map<String, FeatureValue>> queryResultEntry : queryResult.entrySet()) {
                String id = queryResultEntry.getKey();
                if (result.containsKey(id)) {
                    result.get(id).putAll(queryResultEntry.getValue());
                } else {
                    result.put(id, queryResultEntry.getValue());
                }
            }
        }

        FeatureResponse response = FeatureResponse.batchFeatureValues(result);
        return Future.value(response);
    }
}
