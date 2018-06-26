package com.yoho.trace.anaylzer.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by xjipeng on 2017/10/20.
 */
@Data
public class SortedTrace implements Serializable {

    String traceid ;
    String api;
    long duration ;
    List<SpanInfo> sortSpanList ;

    long startMinute ;
    long startDay ;

    boolean errorStatus;
    long traceStartTime;

    int restTemplateTimes;
    int mysqlTimes;
    int redisTimes;
    // 来源于qcloud哪个区
    String region;
}