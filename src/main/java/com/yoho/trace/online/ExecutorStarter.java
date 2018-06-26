package com.yoho.trace.online;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by markeloff on 2017/7/28.
 */
public class ExecutorStarter {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorStarter.class);

    public static void start(ExecutorBuilder executorBuilder) {
        JavaStreamingContext streamingContext = executorBuilder.build();
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            logger.warn("ExecutorStarter catch exception: {}", e);
            streamingContext.stop(true, true);
        }
    }
}
