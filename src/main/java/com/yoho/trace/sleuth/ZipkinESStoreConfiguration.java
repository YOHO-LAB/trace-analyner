package com.yoho.trace.sleuth;

import okhttp3.OkHttpClient;
import properties.ESProperties;
import properties.PropertiesFactory;
import zipkin.collector.CollectorSampler;
import zipkin.storage.StorageComponent;
import zipkin.storage.elasticsearch.http.ElasticsearchHttpStorage;

/**
 * Created by markeloff on 2017/7/26.
 */
public class ZipkinESStoreConfiguration {
    private static ESProperties esProperties = PropertiesFactory.es();

    public static ZipkinMessageListener createZipkinMessageListener() {
        OkHttpClient client = new OkHttpClient.Builder().build();
        ElasticsearchHttpStorage.Builder esHttpBuilder = initESHttpProperties().toBuilder(client)
                .strictTraceId(true)
                .namesLookback(86400000);
        StorageComponent storage = esHttpBuilder.build();
        return new ZipkinMessageListener(storage, CollectorSampler.ALWAYS_SAMPLE);
    }

    private static ZipkinElasticsearchHttpStorageProperties initESHttpProperties() {
        ZipkinElasticsearchHttpStorageProperties zehs = new ZipkinElasticsearchHttpStorageProperties();
        zehs.setHosts(esProperties.getHosts());
        zehs.setIndex(esProperties.getIndex());
        zehs.setIndexShards(esProperties.getIndexShards());
        zehs.setIndexReplicas(esProperties.getIndexReplicas());
        return zehs;
    }
}
