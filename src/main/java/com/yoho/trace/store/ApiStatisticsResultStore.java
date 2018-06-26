package com.yoho.trace.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.yoho.trace.anaylzer.model.ApiTraceResult;

import scala.Tuple2;

/**
 * Created by markeloff on 2017/7/26.
 */
public class ApiStatisticsResultStore {

    private static final Logger logger = LoggerFactory.getLogger(ApiStatisticsResultStore.class);


    public static void store(JavaPairRDD<String, ApiTraceResult> apiResultRdd, String tableName) {
        apiResultRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ApiTraceResult>>>() {
            @Override
            public void call(Iterator<Tuple2<String, ApiTraceResult>> tuple2Iterator) throws Exception {
                HTable resultTable = null;
                try {
                    if (resultTable == null) {
                        resultTable = (HTable) HBasePool.getConnection().getTable(TableName.valueOf(tableName));
                    }

                    if(tuple2Iterator == null){
                        return;
                    }

                    List<Put> putList = new ArrayList<>();
                    long now = System.currentTimeMillis()/1000;
                    while(tuple2Iterator.hasNext()){
                        Tuple2<String, ApiTraceResult> next = tuple2Iterator.next();
                        ApiTraceResult apiTraceResult = next._2;
                        String[] md5Tags = StringUtils.split(apiTraceResult.getTraceMd5(), '.');
                        String md5 = "";

                        if (md5Tags!= null && md5Tags.length==2) {
                            md5 = md5Tags[1];
                        }

                        String rowkey = "";
                        if("trace_api_analyze_minutes".equals(tableName)){
                            rowkey = apiTraceResult.getApiName() + ":" + now + ":" + md5;
                        }else{
                            rowkey = apiTraceResult.getApiName() + ":" + apiTraceResult.getTraceMd5();
                        }

                        Put put = new Put(Bytes.toBytes(rowkey));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("api"), Bytes.toBytes(apiTraceResult.getApiName()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("traceMd5"), Bytes.toBytes(md5));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("duration"), Bytes.toBytes(apiTraceResult.getDuration()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("times"), Bytes.toBytes(apiTraceResult.getCallTimes()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("maxLatency"), Bytes.toBytes(apiTraceResult.getMaxLatency()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("maxLatencyTrace"), Bytes.toBytes(apiTraceResult.getMaxLatencyTrace()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("minLatency"), Bytes.toBytes(apiTraceResult.getMinLatency()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("minLatencyTrace"), Bytes.toBytes(apiTraceResult.getMinLatencyTrace()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("spans"), Bytes.toBytes(JSONObject.toJSONString(apiTraceResult.getSpans())));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("restTemplateTimes"), Bytes.toBytes(apiTraceResult.getRestTemplateTimes()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("redisTimes"), Bytes.toBytes(apiTraceResult.getRedisTimes()));
                        put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("mysqlTimes"), Bytes.toBytes(apiTraceResult.getMysqlTimes()));
                        if(StringUtils.isNotEmpty(apiTraceResult.getRegion())){
                            put.addColumn(Bytes.toBytes("trace"), Bytes.toBytes("region"), Bytes.toBytes(apiTraceResult.getRegion()));
                        }

                        putList.add(put);
                    }

                    resultTable.put(putList);
                } catch (Exception e) {
                    logger.error(e.getMessage(),e);
                } finally {
                    if (resultTable != null)
                        resultTable.close();
                }
            }
        });
    }

}
