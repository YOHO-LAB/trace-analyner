package com.yoho.trace.anaylzer.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by xjipeng on 2017/10/12.
 */
@Data
public class SpanInfo implements Serializable {

    String name ;
    String traceid ;
    String spanid ;
    long begin ;
    long end ;
    String parent ;
    String endpoint ;

    String service ;
    String ip ;

    String srcService;
    String dstService;

    int level ;
    long receive ; //毫秒
    /**
     * 来源页面ID
     */
    private String pageId;

    private String httpHost;
    //该span出现异常次数
    private int errorCount;
    long duration;

    private SpanType spanType;
    // 来源于qcloud哪个区
    private String region;
}