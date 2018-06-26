package com.yoho.trace.store;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.yoho.trace.sleuth.Span;
import com.yoho.trace.sleuth.Spans;

/**
 * Created by markeloff on 2017/7/26.
 */
public class TraceSpanStore implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(TraceSpanStore.class) ;


    public static int store(Spans spans, HTable traceTable) {

        int count = 0 ;

        if (spans != null && CollectionUtils.isNotEmpty(spans.getSpans())) {
            try {

                List<Span> spanList = spans.getSpans() ;
                for(int i=0; i<spanList.size(); i++){

                    Span span = spanList.get(i);
                    StringBuffer sb = new StringBuffer(128);

                    String logEvent = "none" ;
                    if( CollectionUtils.isNotEmpty(span.logs()) ){
                        logEvent = span.logs().get(0).getEvent() ;
                    }

                    String rowkey = sb.append( span.idToHex(span.getTraceId()) + ":" + (span.getBegin()/1000) ).append(":").append(span.idToHex(span.getSpanId()))
                            .append(":").append(logEvent).toString() ;

                    Put put = new Put(Bytes.toBytes(rowkey)) ;
                    //其实不推荐关闭WAL，不过关了的确可以提升性能...因为HBase在写数据前会先写WAL，以保证在异常情况下，HBase可以按照WAL的记录来恢复还未持久化的数据。
                    put.setDurability(Durability.SKIP_WAL);
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("service"),Bytes.toBytes( spans.getHost().getServiceName() )) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("event"),Bytes.toBytes( logEvent )) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("ip"),Bytes.toBytes( spans.getHost().getAddress() )) ;

                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("name"),Bytes.toBytes(span.getName())) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("traceid"),Bytes.toBytes( Span.idToHex(span.getTraceId()) )) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("spanid"),Bytes.toBytes( Span.idToHex(span.getSpanId()) )) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("begin"),Bytes.toBytes( span.getBegin() )) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("end"),Bytes.toBytes( span.getEnd() )) ;

                    if( CollectionUtils.isNotEmpty( span.getParents())) {
                        put.addColumn(Bytes.toBytes("span"), Bytes.toBytes("parent"), Bytes.toBytes(Span.idToHex(span.getParents().get(span.getParents().size() - 1)))) ;
                    }
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("logs"),Bytes.toBytes(JSONObject.toJSONString(span.logs()))) ;
                    put.addColumn(Bytes.toBytes("span"),Bytes.toBytes("tags"),Bytes.toBytes(JSONObject.toJSONString(span.tags()))) ;

                    traceTable.put(put);
                    count++ ;
                }

            }catch(Exception e){
                logger.error("store to hbase failed, spand {} ", JSONObject.toJSONString(spans),e);
            }
        }
        return count ;
    }

}
