package com.newsbreak.data.feature.service.utils;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.utils
 * @className: HttpHelper
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/18 14:45
 * @version: 1.0
 */
public class HttpHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpHelper.class);

    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();

    private static int HTTP_SUCCESS_CODE = 200;

    public static String getJsonStrFromUrl(String url) {
        return getJsonStrFromUrl(url, true);
    }

    /**
     * Http Get request
     * @param url
     * @return json str
     */
    public static String getJsonStrFromUrl(String url, boolean isPrintUrl) {
        if (isPrintUrl) {
            LOGGER.info("HttpGet: {}", url);
        }
        HttpGet get = new HttpGet(url);
        CloseableHttpResponse response = null;
        String result = null;
        try {
            response = HTTP_CLIENT.execute(get);

            HttpEntity entity = response.getEntity();
            if (entity != null) {
                result = EntityUtils.toString(entity);
            }

            if (response.getStatusLine().getStatusCode() != HTTP_SUCCESS_CODE) {
                throw new Exception(String.format("Get request error. Status code: %d, msg: %s", response.getStatusLine().getStatusCode(), result));
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            result = null;
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        LOGGER.info("Result: {}", result);
        return result;
    }
}
