package com.yoho.trace.sleuth.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.yoho.trace.sleuth.Span;
import com.yoho.trace.sleuth.Spans;
import com.yoho.trace.sleuth.ZipkinESStoreConfiguration;
import com.yoho.trace.sleuth.ZipkinMessageListener;
import org.apache.commons.collections.CollectionUtils;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by markeloff on 2017/7/26.
 */
public class TraceStoreUtils {
    private static ZipkinMessageListener zipkinMessageListener;

    private static final Logger logger = LoggerFactory.getLogger(TraceStoreUtils.class);

    static {
        zipkinMessageListener = ZipkinESStoreConfiguration.createZipkinMessageListener();

    }

    public static void store(Spans spans) {
        if (spans != null && CollectionUtils.isNotEmpty(spans.getSpans())) {
            //zipkinMessageListener.sink(spans);
        }
    }




}
