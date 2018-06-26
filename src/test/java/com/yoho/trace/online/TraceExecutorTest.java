package com.yoho.trace.online;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import properties.PropertiesFactory;

/**
 * Created by markeloff on 2017/7/28.
 */
public class TraceExecutorTest {
    private static final String CHECKPOINT_FS_DIR = "/Users/xjipeng/code/spark/checkpoint7";


    public static void main(String[] args) {

        ExecutorBuilder executorBuilder = new ExecutorBuilder().
                checkpointDir(CHECKPOINT_FS_DIR).
                sparkConf("local[4]").
                kafkaParamMap().
                javaStreamingContext();

        JavaStreamingContext streamingContext = executorBuilder.build();

        streamingContext.start();

        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            //logger.warn("ExecutorStarter catch exception: {}", e);
            streamingContext.stop(true, true);
        }
    }
}
