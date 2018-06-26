package com.yoho.trace.offline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import properties.PropertiesFactory;


/**
 * Created by xjipeng on 2017/10/4.
 */


public class TraceExecutor {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("TraceExecutor");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        AnalyzeHandleFactory.start(jsc);

        Thread.sleep(600000);

        jsc.stop();
    }

}
