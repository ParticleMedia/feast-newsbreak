package com.newsbreak.data.feature.service;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.twitter.finagle.Service;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.client.StackBasedClient;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.finagle.thrift.service.ThriftResponseClassifier;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @projectName: feature-service
 * @package: com.newsbreak.data.feature.service
 * @className: FeatureServiceClient
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/9/20 11:25
 * @version: 1.0
 */
public class FeatureServiceClient {

    private Logger logger = LoggerFactory.getLogger(FeatureServiceClient.class);

    private final String host = "localhost"; //"data-feature-service-server-svc";
    private final int port = 9999;

    private FeatureService.ServiceIface featureService;

    public FeatureServiceClient(String clientId) {

        logger.info("host={}, port={}", host, port);

        StackBasedClient<ThriftClientRequest, byte[]> stackedClient = Thrift.client()
//                .withExecutionOffloaded(executorService)
//                .withLoadBalancer().apply(getLoadBalancerFactory(config.getLoadBalancer()))
                .withClientId(ClientId.apply(clientId));

        Service<ThriftClientRequest, byte[]> service = ClientBuilder.safeBuild(
                ClientBuilder.get()
                        .stack(stackedClient)
                        .name(clientId)
                        .connectTimeout(Duration.fromSeconds(5))
                        .tcpConnectTimeout(Duration.fromSeconds(5))
                        .requestTimeout(Duration.fromSeconds(5))
                        .timeout(Duration.fromSeconds(5))
//                        .retries(3)
//                        .keepAlive(true)
                        .failFast(true)
                        .noFailureAccrual()
                        .responseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures())
                        .dest(host + ":" + port)
        );

        featureService = new FeatureService.ServiceToClient(service);
    }

    public Future<FeatureResponse> getFeatures(Map<String, String> key, List<String> features) {
        Future<FeatureResponse> future = featureService.getFeatures(new FeatureRequest(features, key));
        return future;
    }

    public Future<FeatureResponse> batchGetFeatures(String keyName, List<String> keys, List<String> features) {
        Future<FeatureResponse> future = featureService.batchGetFeatures(new BatchFeatureRequest(features, keyName, keys));
        return future;
    }

    public static void main( String[] args ) throws Exception {
        Logger logger = LoggerFactory.getLogger("feature_service");

        FeatureServiceClient client = new FeatureServiceClient("thrift_client");
        Map<String, String> key = new HashMap<>();
        key.put("user_id", "165292122");
        key.put("user_id", "198063370");
        Future<FeatureResponse> future = client.getFeatures(key,
                Arrays.asList("nova_ads_user_feature:union_stddev_impressions", "nova_ads_user_feature:click_ads_emb_sum_d"));

        FeatureResponse response = Await.result(future, Duration.fromSeconds(60));
        System.out.println("received result.");
        for (Map.Entry<String, FeatureValue> entry : response.getFeatureValues().entrySet()) {
            logger.info("key={}, Value={}", entry.getKey(), entry.getValue());
        }

        // batch get
        Future<FeatureResponse> batchFuture = client.batchGetFeatures("user_id",
                Lists.newArrayList("165292122", "198063370", "20847207"),
                Arrays.asList("nova_ads_user_feature:union_stddev_impressions", "nova_ads_user_feature:click_ads_emb_sum_d"));
        FeatureResponse batchResponse = Await.result(batchFuture, Duration.fromSeconds(60));
        System.out.println("received result.");
        for (Map.Entry<String, Map<String, FeatureValue>> entry : batchResponse.getBatchFeatureValues().entrySet()) {
            String id = entry.getKey();
            for (Map.Entry<String, FeatureValue> valueEntry : entry.getValue().entrySet()) {
                logger.info("id={}, key={}, Value={}", id, valueEntry.getKey(), valueEntry.getValue());
            }
        }
    }
}
