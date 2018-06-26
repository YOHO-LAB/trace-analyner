package com.yoho.trace.online.handler;

import com.yoho.trace.sleuth.Spans;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Created by markeloff on 2017/7/26.
 */
public interface TraceHandler {
    void handle(final JavaDStream<Spans> kafkaMsgDStream);
}
