package com.nb.data;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.newsbreak.data.feature.service.service.meta.FeatureMetaService;
import com.newsbreak.data.feature.service.service.meta.UnifiedFeatureMetaService;
import com.newsbreak.data.feature.service.service.meta.UnifiedFeatureMetaServiceImpl;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.serving.registry.LocalRegistryFile;
import feast.serving.registry.RegistryRepository;
import feast.serving.registry.S3RegistryFile;
import feast.serving.service.config.ApplicationProperties;
import org.junit.Before;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.BeforeClass;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;

/**
 * Created by ricky on 2022/11/11.
 */
@Testcontainers
public class UnifiedFeatureMetaServiceTest {
    private static UnifiedFeatureMetaService unifiedFeatureMetaService;

    @Container static final S3MockContainer s3Mock = new S3MockContainer("2.2.3");
    static RegistryProto.Registry registryProto = readLocalRegistry();

    private static AmazonS3 createClient() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                String.format("http://localhost:%d", s3Mock.getHttpServerPort()), "us-east-1"))
                .enablePathStyleAccess()
                .build();
    }

    private static void putToStorage(RegistryProto.Registry proto) {
        byte[] bytes = proto.toByteArray();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setContentType("application/protobuf");

        AmazonS3 s3Client = createClient();
        s3Client.putObject("test-bucket", "registry.db", new ByteArrayInputStream(bytes), metadata);
    }

    private static ApplicationProperties.FeastProperties createBasicFeastProperties(
            String scyllaHost, Integer scyllaPort) {
        final ApplicationProperties.FeastProperties feastProperties =
                new ApplicationProperties.FeastProperties();
        feastProperties.setRegistry("src/test/resources/feature_repo.db");
        feastProperties.setRegistryRefreshInterval(1);

        feastProperties.setActiveStore("online");
        feastProperties.setProject("feast_project");
        feastProperties.setEntityKeySerializationVersion(2);
        feastProperties.setStores(
                ImmutableList.of(
                        new ApplicationProperties.Store(
                                "online",
                                "cassandra",
                                ImmutableMap.of(
                                        "host", "localhost", "port", "9042", "password", "testpw"))));

        return feastProperties;
    }

    private static RegistryProto.Registry readLocalRegistry() {
        return new LocalRegistryFile("src/test/resources/feature_repo.db")
                .getContent();
    }


    ApplicationProperties.FeastProperties createFeastProperties() {
        final ApplicationProperties.FeastProperties feastProperties =
                createBasicFeastProperties(
                        "localhost", 9042);
        feastProperties.setRegistry("s3://test-bucket/registry.db");

        return feastProperties;
    }

    void updateRegistryFile(RegistryProto.Registry registry) {
        putToStorage(registry);
    }

    AbstractModule registryConfig() {
        return new AbstractModule() {
            @Provides
            public AmazonS3 awsStorage() {
                return AmazonS3ClientBuilder.standard()
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(
                                        String.format("http://localhost:%d", s3Mock.getHttpServerPort()), "us-east-1"))
                        .enablePathStyleAccess()
                        .build();
            }
        };
    }

    private static AmazonS3 getMockAmazonS3() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                String.format("http://localhost:%d", s3Mock.getHttpServerPort()), "us-east-1"))
                .enablePathStyleAccess()
                .build();
    }

    @BeforeAll
    public static void init() {
        AmazonS3 s3Client = createClient();
        s3Client.createBucket("test-bucket");

        putToStorage(registryProto);

        S3RegistryFile registryFile = new S3RegistryFile(getMockAmazonS3(), "s3://test-bucket/registry.db");


        FeatureMetaService featureMetaService = new FeatureMetaService();
        RegistryRepository registryRepository = new RegistryRepository(registryFile, 1000);


        unifiedFeatureMetaService = new UnifiedFeatureMetaServiceImpl(featureMetaService, registryRepository);
    }

    @Test
    public void test1() {
        Assertions.assertNotNull(unifiedFeatureMetaService.getFeastFeatureMetaRepo());
        Assertions.assertNotNull(unifiedFeatureMetaService.getInternalFeatureMetaRepo());

        //Test feast default meta
        ServingAPIProto.FeatureReferenceV2 featureViewReference = ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("nova_user_base_features_daily_view")
                .build();
        Assertions.assertNotEquals(0, unifiedFeatureMetaService.getFeastFeatureMetaRepo().getEntitiesList(featureViewReference));
        Assertions.assertNotNull(unifiedFeatureMetaService.getFeastFeatureMetaRepo().getFeatureViewSpec(featureViewReference));

    }
}
