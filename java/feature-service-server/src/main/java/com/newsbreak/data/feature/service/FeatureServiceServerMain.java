package com.newsbreak.data.feature.service;

import com.google.common.collect.Lists;
import com.google.inject.Module;
import com.newsbreak.data.feature.service.controller.FeatureServiceController;
import com.newsbreak.data.feature.service.module.ControllerModule;
import com.newsbreak.data.feature.service.module.FeatureMetaModule;
import com.twitter.finagle.ThriftMux;
import com.twitter.finatra.thrift.AbstractThriftServer;
import com.twitter.finatra.thrift.filters.*;
import com.twitter.finatra.thrift.routing.JavaThriftRouter;

import java.util.Collection;

/**
 * @projectName: feature-service
 * @package: com.newsbreak.data.feature.service
 * @className: FeatureServiceServerMain
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/9/22 15:17
 * @version: 1.0
 */
public class FeatureServiceServerMain extends AbstractThriftServer {

    @Override
    public Collection<Module> javaModules() {
        return Lists.newArrayList(
                new ControllerModule(),
                new FeatureMetaModule()
        );
    }

    @Override
    public void configureThrift(JavaThriftRouter router) {
        router.filter(LoggingMDCFilter.class)
                .filter(TraceIdMDCFilter.class)
                .filter(ThriftMDCFilter.class)
                .filter(AccessLoggingFilter.class)
                .filter(StatsFilter.class)
                .add(FeatureServiceController.class);
    }

    @Override
    public ThriftMux.Server configureThriftServer(ThriftMux.Server server) {
        ThriftMux.Server configured = server
                .withMaxReusableBufferSize(500000);
        return configured;
    }
}
