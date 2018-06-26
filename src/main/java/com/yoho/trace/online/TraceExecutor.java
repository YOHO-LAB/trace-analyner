package com.yoho.trace.online;


/**
 * Created by markeloff on 2017/7/26.
 */
public class TraceExecutor {
    public static void main(String[] args) {

        ExecutorBuilder executorBuilder = new ExecutorBuilder().
                sparkConf().
                kafkaParamMap().
                javaStreamingContext();
        ExecutorStarter.start(executorBuilder);
    }


}
