package com.yoho.trace.offline.handler;

import com.yoho.trace.anaylzer.model.SpanInfo;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Created by xjipeng on 2017/10/24.
 */
public interface IAnalyzeHandler {
    void handle(JavaPairRDD<String, SpanInfo> hBaseTracePairRDD);
}
