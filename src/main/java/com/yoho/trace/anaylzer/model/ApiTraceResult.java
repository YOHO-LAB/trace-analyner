package com.yoho.trace.anaylzer.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by xjipeng on 2017/10/19.
 */
@Data
public class ApiTraceResult implements Serializable {

    // api 名称
    String apiName;

    int callTimes ;  // api 调用次数
    long duration ;   // 平均处理时间

    List<SpanResult> spans ;  // 调用链的span 信息

    String traceMd5 ; //每种类型一个 trace link type

    String maxLatencyTrace;   //延时最长的traceid
    String minLatencyTrace ;  //延时最短的traceid

    int maxLatency ;
    int minLatency ;

    String prefex ;

    String traceId;

    boolean errorStatus;

    long traceStartTime;

    int restTemplateTimes;

    int mysqlTimes;

    int redisTimes;
    // 来源于qcloud哪个区
    String region;
}