package com.yoho.trace.anaylzer.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by xjipeng on 2017/10/19.
 */
@Data
public class SpanResult implements Serializable {

    String spanName ;
    String spanId ;
    long duration ;
    int level ;
    String parent ;

    //服务调用的 源服务 & 目的服务
    String srcService ;
    String dstService ;

    List<String> srcIp ;
    List<String> dstIp ;
    int errorCount;
    SpanType spanType;


    public SpanResult(String spanName, long duration,int level,String spanId, String parent, String srcService, String dstService, List<String> srcIp, List<String> dstIp,int errorCount,SpanType spanType  ){
        this.spanName = spanName ;
        this.spanId = spanId ;
        this.duration = duration ;
        this.level = level ;
        this.parent = parent ;

        this.srcService = srcService ;
        this.dstService = dstService ;
        this.srcIp = srcIp ;
        this.dstIp = dstIp ;
        this.errorCount = errorCount;
        this.spanType=spanType;
    }

}
