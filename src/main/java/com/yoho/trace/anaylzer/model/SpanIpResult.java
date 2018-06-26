package com.yoho.trace.anaylzer.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by wangning on 2017/11/15.
 */
@Data
public class SpanIpResult implements Serializable {
    private long avgDuration;
    private String ip;
    private int times;
}
