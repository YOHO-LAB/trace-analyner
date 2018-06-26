package com.yoho.trace.online;

import com.yoho.trace.offline.AnalyzeHandleFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import properties.PropertiesFactory;


/**
 * Created by xjipeng on 2017/10/4.
 */


public class TraceAnalyzerTest {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("TraceExecutor").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        AnalyzeHandleFactory.start(jsc);

        jsc.stop();
    }

}
