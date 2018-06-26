package com.yoho.trace.online.handler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.clearspring.analytics.util.Lists;
import com.yoho.trace.anaylzer.ApiStatisticsAnalyzer;
import com.yoho.trace.anaylzer.model.ApiTraceResult;
import com.yoho.trace.anaylzer.model.SortedTrace;
import com.yoho.trace.anaylzer.model.SpanInfo;
import com.yoho.trace.anaylzer.model.SpanIpResult;
import com.yoho.trace.anaylzer.model.SpanType;
import com.yoho.trace.sleuth.Span;
import com.yoho.trace.sleuth.Spans;
import com.yoho.trace.store.ApiStatisticsResultStore;
import com.yoho.trace.store.HBasePool;

import scala.Tuple2;

/**
 * Created by markeloff on 2017/7/26.
 */
public class TraceAnalyzeHandler implements TraceHandler, Serializable {

    private ApiStatisticsAnalyzer analyzer;

    private static Logger logger = LoggerFactory.getLogger(TraceAnalyzeHandler.class);

    public TraceAnalyzeHandler() {
        analyzer = new ApiStatisticsAnalyzer(false);
    }

    private static final int interval = 3000;  //3s

    @Override
    public void handle(final JavaDStream<Spans> kafkaMsgDStream) {

        // key 为 traceid， value 为 每个 span
        JavaPairDStream<String, SpanInfo> spanPairDStream = kafkaMsgDStream.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Spans>, String, SpanInfo>() {

            @Override
            public Iterator<Tuple2<String, SpanInfo>> call(Iterator<Spans> spansIterator) throws Exception {

                long enter = System.currentTimeMillis();
                List<Tuple2<String, SpanInfo>> spanInfoList = new ArrayList<>(500);
                while (spansIterator.hasNext()) {
                    Spans spans = spansIterator.next();
                    spans.getHost();
                    List<Span> list = spans.getSpans();
                    Iterator<Span> itor = list.iterator();
                    while (itor.hasNext()) {
                        Span span = itor.next();

                        String logEvent = "none";
                        if (CollectionUtils.isNotEmpty(span.logs())) {
                            logEvent = span.logs().get(0).getEvent();
                        }

                        SpanInfo spanInfo = new SpanInfo();
                        spanInfo.setName(span.getName());
                        spanInfo.setBegin(span.getBegin());
                        spanInfo.setEnd(span.getEnd());
                        spanInfo.setDuration(span.getEnd() - span.getBegin());
                        spanInfo.setTraceid(Span.idToHex(span.getTraceId()));
                        spanInfo.setSpanid(Span.idToHex(span.getSpanId()));
                        if (span.getParents().size() > 0) {
                            spanInfo.setParent(Span.idToHex(span.getParents().get(span.getParents().size() - 1)));
                        }
                        spanInfo.setService(spans.getHost().getServiceName());
                        spanInfo.setEndpoint(logEvent);
                        spanInfo.setIp(spans.getHost().getAddress());
                        spanInfo.setReceive(spans.getReceive());
                        if(span.tags()!=null){
                            spanInfo.setHttpHost(span.tags().get("http.host"));
                            //标记span是否是是异常span
                            if(StringUtils.isNotBlank(span.tags().get("error"))){
                                spanInfo.setErrorCount(1);
                            }

                            String lc = span.tags().get("lc");
                            String httpUrl = span.tags().get("http.url");
                            if("redis".equals(lc)){
                                spanInfo.setSpanType(SpanType.REDIS);
                            }else if("mysql".equals(lc)){
                                spanInfo.setSpanType(SpanType.MYSQL);
                            }else if(StringUtils.isNoneBlank(httpUrl)){
                                spanInfo.setSpanType(SpanType.RESTTEMPLATE);
                            }else if(StringUtils.equals(spanInfo.getTraceid(),spanInfo.getSpanid())){
                                spanInfo.setSpanType(SpanType.HTTP);
                            }else{
                                spanInfo.setSpanType(SpanType.OTHER);
                            }

                            if(StringUtils.isNotBlank(span.tags().get("yoho.region"))){
                                spanInfo.setRegion(span.tags().get("yoho.region"));
                            }
                        }

                        spanInfoList.add(new Tuple2<>(spanInfo.getTraceid(), spanInfo));
                    }
                }

                logger.info("kafkaMsgDStream.mapPartitionsToPair elapse {}, size {} ", System.currentTimeMillis() - enter, spanInfoList.size());

                return spanInfoList.iterator();
            }
        });


        //把 span 按照 traceid 进行分组，  并过滤 120s 以前的 trace，不进行分析
        JavaPairDStream<String, Iterable<SpanInfo>> tracePairDStream = spanPairDStream.groupByKey().filter(new Function<Tuple2<String, Iterable<SpanInfo>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<SpanInfo>> v1) throws Exception {
                Iterator<SpanInfo> itr = v1._2().iterator();
                long current = System.currentTimeMillis();
                if (itr.hasNext()) {
                    SpanInfo span = itr.next();
                    //过滤过早的调用信息，不进行分析
                    if (current - span.getEnd() >= 120000) {
                        //logger.info("filter early trace , don't anaylze it, trace id {}", v1._1());
                        return false;
                    }
                }
                return true;
            }
        });


        //返回的 dstream （mapWithStateDStream）中，是立即进行处理的， state 中保存的是 上个周期的最后3秒和本周期的最后3秒
        JavaMapWithStateDStream mapWithStateDStream = tracePairDStream.mapWithState(StateSpec.function(new Function3<String, Optional<Iterable<SpanInfo>>,
                State<Iterable<SpanInfo>>, Tuple2<String, Iterable<SpanInfo>>>() {
            @Override
            public Tuple2<String, Iterable<SpanInfo>> call(String traceId, Optional<Iterable<SpanInfo>> current, State<Iterable<SpanInfo>> state) throws Exception {
                boolean isLast3sRecv = false;
                List<SpanInfo> newSpanList = new ArrayList<SpanInfo>();
                if (current.isPresent()) {
                    Iterator itor = current.get().iterator();
                    while (itor.hasNext()) {
                        SpanInfo info = (SpanInfo) itor.next();
                        if ((info.getReceive() - info.getEnd()) < interval) {
                            isLast3sRecv = true;
                        }
                        newSpanList.add(info);
                    }
                }
                // 1 只要有span出现在本批次最后3秒，全部更新的state中，不处理， return 空
                if (isLast3sRecv) {
                    if (CollectionUtils.isNotEmpty(newSpanList)) {
                        logger.info("latest 3s trace in current batch, just state, trace id {}, span count {}", traceId, newSpanList.size());
                        state.update(newSpanList);
                    }
                    //null会被过滤掉，本批次不处理
                    return new Tuple2<>(traceId, null);
                }
                // 2 state 中有的trace，合并spanlist, 清理state， return完整的trace spanlist
                if (state.exists()) {
                    Iterator itor = state.get().iterator();
                    while (itor.hasNext()) {
                        newSpanList.add((SpanInfo) itor.next());
                    }

                    if (!state.isTimingOut()) {
                        //state未超时
                        state.remove();
                        logger.info("full trace with last batch spans, trace id {}, span count {} ", traceId, newSpanList.size());
                        return new Tuple2<String, Iterable<SpanInfo>>(traceId, newSpanList);
                    } else {
                        //state已超时
                        logger.info("state is timeout, can not process, trace id {}, span count {} ", traceId, newSpanList.size());
                        //null会被过滤掉，不处理
                        return new Tuple2<>(traceId, null);
                    }
                }

                // 3 state中没有的 trace，并且不在最后3秒，return完整的trace spanlist
                logger.info("full trace in current batch, trace id {}, span count {} ", traceId, newSpanList.size());
                return new Tuple2<String, Iterable<SpanInfo>>(traceId, newSpanList);

            }

        }).timeout(Durations.seconds(90)));

        //过滤
        JavaPairDStream<String, Iterable<SpanInfo>> currentTraceStream = mapWithStateDStream.filter(new Function<Tuple2<String, Iterable<SpanInfo>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<SpanInfo>> v1) throws Exception {
                //本周期最后3秒收到的trace，等到下个周期再处理
                if (v1._2() == null) {
                    logger.info("filter null trace (latest 3s will process on next batch) , trace id {}", v1._1());
                    return false;
                }
                logger.info("go to analyze current batch trace, trace id {}", v1._1());
                return true;
            }
        }).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<SpanInfo>>>, String, Iterable<SpanInfo>>() {
            @Override
            public Iterator<Tuple2<String, Iterable<SpanInfo>>> call(Iterator<Tuple2<String, Iterable<SpanInfo>>> tuple2Iterator) throws Exception {
                return tuple2Iterator;
            }
        });


        //mapWithStateDStream.stateSnapshots    state 中 保存了 1、本批次最后3秒， 2、 上个批次最后3秒（本批次未更新的trace）
        JavaPairDStream<String, Iterable<SpanInfo>> lastStateTraceStream = mapWithStateDStream.stateSnapshots()
                .filter(new Function<Tuple2<String, Iterable<SpanInfo>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Iterable<SpanInfo>> v1) throws Exception {

                        long current = System.currentTimeMillis();
                        Iterator itor = v1._2().iterator();
                        while (itor.hasNext()) {
                            SpanInfo span = (SpanInfo) itor.next();
                            //过滤 本批次的， 只保留上个批次的
                            if ((current - span.getReceive()) < 60000) {
                                logger.info("filter latest 3s trace from state snapshot, trace id {}", v1._1());
                                //本批次内收到的，不处理，因为state中，1、本批次最后3秒， 2、 上个批次最后3秒（本批次未更新的trace）
                                return false;
                            }
                            //过滤 上个批次之前的 ， 只保留 上个批次的 ，因为 state 只在 checkpoint 的时候，才进行timeout 清理
                            //如果不过滤，就会有多个批次的数据， 导致重复分析
                            if ((current - span.getReceive()) >= 120000) {
                                logger.info("filter last 120s trace from state snapshot, trace id {}", v1._1());
                                return false;
                            }
                        }
                        //只保留， 60秒以上， 120秒以下的 trace信息， 即上个批次遗留下来，需要处理的trace
                        logger.info("go to analyze last batch trace (latest 3s on last batch), trace id {}", v1._1());
                        return true;
                    }
                });


        //key traceid
        JavaPairDStream<String, Iterable<SpanInfo>> needAnaylzeStream = currentTraceStream.union(lastStateTraceStream);

        //key minuteTime.traceMD5 value 该traceMD5的一个traceid对应的信息
        JavaPairDStream<String, SortedTrace> sortSpanTrace = needAnaylzeStream.flatMapToPair(analyzer.SortSpanTreeFunc);

        sortSpanTrace.cache();

        JavaPairDStream<String, ApiTraceResult> apiResultTraceDStream = sortSpanTrace.mapToPair(analyzer.ConvertTraceResultFunc) ;
        apiResultTraceDStream.cache();
        
        //1处理异常调用链信息
        handlerExceptionTrace(apiResultTraceDStream);
        
        //2处理treemd5调用链总览
        handlertraceApiAnalyzeMinutes(apiResultTraceDStream);

        //3处理所有traceid
        handlerAllTrace(apiResultTraceDStream);
        
        //4处理span+ip的耗时分布
        handlerSpanIp(sortSpanTrace);
    }


	private void handlerSpanIp(JavaPairDStream<String, SortedTrace> sortSpanTrace) {
		// key traceMD5 + ":" + spanName + ":" + minuteStart + ":" + ip
		JavaPairDStream<String, SpanIpResult> stringSpanInfoJavaPairDStream = sortSpanTrace.mapPartitionsToPair(
				new PairFlatMapFunction<Iterator<Tuple2<String, SortedTrace>>, String, SpanIpResult>() {
					@Override
					public Iterator<Tuple2<String, SpanIpResult>> call(Iterator<Tuple2<String, SortedTrace>> tuple2List)
							throws Exception {
						if (null == tuple2List) {
							return null;
						}
						List<Tuple2<String, SpanIpResult>> resultList = Lists.newArrayList();
						while (tuple2List.hasNext()) {
							Tuple2<String, SortedTrace> tuple2 = tuple2List.next();
							String minuteStart = StringUtils.split(tuple2._1, '.')[0];
							String traceMD5 = StringUtils.split(tuple2._1, '.')[1];
							SortedTrace sortTrace = tuple2._2;
							List<SpanInfo> sortSpanList = sortTrace.getSortSpanList();
							for (SpanInfo spanInfo : sortSpanList) {
								String ip = "";
								// 只有root和resttemplate才会生成ip
								if (spanInfo.getTraceid().equals(spanInfo.getSpanid())) {
									ip = spanInfo.getIp();
								} else {
									if (StringUtils.isNotBlank(spanInfo.getHttpHost())) {
										ip = spanInfo.getHttpHost();
									}
								}
								if (StringUtils.isBlank(ip)) {
									continue;
								}
								SpanIpResult spanIpResult = new SpanIpResult();
								String spanName = spanInfo.getName();
								long duration = spanInfo.getEnd() - spanInfo.getBegin();
								spanIpResult.setAvgDuration(duration);
								spanIpResult.setIp(ip);
								spanIpResult.setTimes(1);
								String key = traceMD5 + ":" + spanName + ":" + minuteStart + ":" + ip;
								Tuple2<String, SpanIpResult> resultTuple2 = new Tuple2<>(key, spanIpResult);
								resultList.add(resultTuple2);
							}
						}
						return resultList.iterator();
					}
				});
		

        JavaPairDStream<String, SpanIpResult> stringSpanIpResultJavaPairDStream = stringSpanInfoJavaPairDStream.reduceByKey(new Function2<SpanIpResult, SpanIpResult, SpanIpResult>() {
            @Override
            public SpanIpResult call(SpanIpResult v1, SpanIpResult v2) throws Exception {
                SpanIpResult result = new SpanIpResult();
                result.setIp(v1.getIp());
                result.setTimes(v1.getTimes() + v2.getTimes());
                result.setAvgDuration((v1.getAvgDuration() * v1.getTimes() + v2.getAvgDuration() * v2.getTimes()) / result.getTimes());
                return result;
            }
        });


        stringSpanIpResultJavaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, SpanIpResult>>() {
            @Override
            public void call(JavaPairRDD<String, SpanIpResult> stringSpanIpResultJavaPairRDD) throws Exception {
                stringSpanIpResultJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, SpanIpResult>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, SpanIpResult>> tuple2Iterator) throws Exception {

                        long begin = System.currentTimeMillis();
                        int size = 0;

                        HTable resultTable = null;
                        try {
                            if (resultTable == null) {
                                resultTable = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("span_ip"));
                            }

                            if(tuple2Iterator == null){
                                return;
                            }

                            List<Put> putList = new ArrayList<>();
                            while(tuple2Iterator.hasNext()){
                                Tuple2<String, SpanIpResult> next = tuple2Iterator.next();
                                String key = next._1;
                                SpanIpResult spanIpResult = next._2;

                                Put put = new Put(Bytes.toBytes(key));
                              //其实不推荐关闭WAL，不过关了的确可以提升性能...因为HBase在写数据前会先写WAL，以保证在异常情况下，HBase可以按照WAL的记录来恢复还未持久化的数据。
                                put.setDurability(Durability.SKIP_WAL);
                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("avg_duration"), Bytes.toBytes(spanIpResult.getAvgDuration()));
                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("times"), Bytes.toBytes(spanIpResult.getTimes()));
                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ip"), Bytes.toBytes(spanIpResult.getIp()));
                                putList.add(put);
                            }
                            size = putList.size();
                            if(CollectionUtils.isNotEmpty(putList)) {
                            	resultTable.put(putList);
                            }
                            

                        } catch (Exception e) {
                            logger.error(e.getMessage(),e);
                        } finally {
                            if (resultTable != null)
                                resultTable.close();
                        }



                        long end = System.currentTimeMillis();

                        System.out.println(new Date() + "hbase span_ip inset time " + (end - begin) + " size is " + size);

                    }
                });
            }
        });
    }


    private void handlertraceApiAnalyzeMinutes(JavaPairDStream<String, ApiTraceResult> apiResultTraceDStream){
        //根据traceMD5分组计算value
        JavaPairDStream<String, ApiTraceResult> resultDStream = apiResultTraceDStream.reduceByKey(analyzer.ReduceFunc) ;

        resultDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, ApiTraceResult>>() {
            @Override
            public void call(JavaPairRDD<String, ApiTraceResult> apiResultRdd) throws Exception {

                ApiStatisticsResultStore.store(apiResultRdd, "trace_api_analyze_minutes");
            }
        });
    }

    private void handlerExceptionTrace(JavaPairDStream<String, ApiTraceResult> apiResultTraceDStream){
        JavaPairDStream<String, ApiTraceResult> filter = apiResultTraceDStream.filter(new Function<Tuple2<String, ApiTraceResult>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, ApiTraceResult> stringSortedTraceTuple2) throws Exception {
                if(stringSortedTraceTuple2._2.isErrorStatus()){
                    if(StringUtils.isBlank(stringSortedTraceTuple2._2.getApiName()) || StringUtils.equals("http:/",stringSortedTraceTuple2._2.getApiName())){
                        return false;
                    }
                    return true;
                }else{
                    return false;
                }
            }
        });




        filter.foreachRDD(new VoidFunction<JavaPairRDD<String, ApiTraceResult>>() {
            @Override
            public void call(JavaPairRDD<String, ApiTraceResult> stringSortedTraceJavaPairRDD) throws Exception {
                stringSortedTraceJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ApiTraceResult>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, ApiTraceResult>> tuple2Iterator) throws Exception {

                        HTable resultTable = null;
                        try {
                            if(tuple2Iterator == null){
                                return;
                            }

                            if (resultTable == null) {
                                resultTable = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("exception_trace"));
                            }

                            List<Put> putList = new ArrayList<>();
                            while(tuple2Iterator.hasNext()){
                                Tuple2<String, ApiTraceResult> next = tuple2Iterator.next();
                                ApiTraceResult apiTraceResult = next._2;

                                Put put = new Put(Bytes.toBytes(apiTraceResult.getTraceStartTime()/1000 + ":" + apiTraceResult.getTraceId()));
                              //其实不推荐关闭WAL，不过关了的确可以提升性能...因为HBase在写数据前会先写WAL，以保证在异常情况下，HBase可以按照WAL的记录来恢复还未持久化的数据。
                                put.setDurability(Durability.SKIP_WAL);
                                //put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("spans"), Bytes.toBytes(JSONObject.toJSONString(apiTraceResult.getSpans())));
                                //put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("traceid"), Bytes.toBytes(apiTraceResult.getTraceId()));
                                //put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("starttime"), Bytes.toBytes(apiTraceResult.getTraceStartTime()/1000));
                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("api"), Bytes.toBytes(apiTraceResult.getApiName()));
                                if(StringUtils.isNotEmpty(apiTraceResult.getRegion())){
                                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(apiTraceResult.getRegion()));
                                }

                                Put put2 = new Put(Bytes.toBytes(apiTraceResult.getApiName() + ":" + apiTraceResult.getTraceStartTime()/1000 + ":" + apiTraceResult.getTraceId()));
                              //其实不推荐关闭WAL，不过关了的确可以提升性能...因为HBase在写数据前会先写WAL，以保证在异常情况下，HBase可以按照WAL的记录来恢复还未持久化的数据。
                                put2.setDurability(Durability.SKIP_WAL);
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("spans"), Bytes.toBytes(JSONObject.toJSONString(apiTraceResult.getSpans())));
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("traceid"), Bytes.toBytes(apiTraceResult.getTraceId()));
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("starttime"), Bytes.toBytes(apiTraceResult.getTraceStartTime()/1000));
                                if(StringUtils.isNotEmpty(apiTraceResult.getRegion())){
                                    put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(apiTraceResult.getRegion()));
                                }

                                putList.add(put);
                                putList.add(put2);
                            }
                            
                            if(CollectionUtils.isNotEmpty(putList)) {
                                resultTable.put(putList);
                            }


                        } catch (Exception e) {
                            logger.error(e.getMessage(),e);
                        } finally {
                            if (resultTable != null)
                                resultTable.close();
                        }
                    }
                });
            }
        });
    }



    private void handlerAllTrace(JavaPairDStream<String, ApiTraceResult> apiResultTraceDStream){
        apiResultTraceDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, ApiTraceResult>>() {
            @Override
            public void call(JavaPairRDD<String, ApiTraceResult> stringSortedTraceJavaPairRDD) throws Exception {

                stringSortedTraceJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ApiTraceResult>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, ApiTraceResult>> tuple2Iterator) throws Exception {

                        long begin = System.currentTimeMillis();
                        int size = 0;

                        HTable resultTable = null;
                        try {
                            if(tuple2Iterator == null){
                                return;
                            }

                            if (resultTable == null) {
                                resultTable = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("all_trace"));
                            }

                            List<Put> putList = new ArrayList<>();
                            while(tuple2Iterator.hasNext()){
                                Tuple2<String, ApiTraceResult> next = tuple2Iterator.next();
                                ApiTraceResult apiTraceResult = next._2;
                                String[] md5Tags = StringUtils.split(apiTraceResult.getTraceMd5(), '.');

                                Put put1 = new Put(Bytes.toBytes( md5Tags[1] + ":" + apiTraceResult.getTraceStartTime()/1000 + ":" + apiTraceResult.getTraceId()));
                              //其实不推荐关闭WAL，不过关了的确可以提升性能...因为HBase在写数据前会先写WAL，以保证在异常情况下，HBase可以按照WAL的记录来恢复还未持久化的数据。
                                put1.setDurability(Durability.SKIP_WAL);
                                put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("spans"), Bytes.toBytes(JSONObject.toJSONString(apiTraceResult.getSpans())));
                                put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("traceid"), Bytes.toBytes(apiTraceResult.getTraceId()));
                                put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("starttime"), Bytes.toBytes(apiTraceResult.getTraceStartTime()/1000));
                                put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("traceMd5"), Bytes.toBytes(apiTraceResult.getTraceMd5()));
                                put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("duration"), Bytes.toBytes(apiTraceResult.getDuration()));
                                if(StringUtils.isNotEmpty(apiTraceResult.getRegion())){
                                    put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(apiTraceResult.getRegion()));
                                }

                                Put put2 = new Put(Bytes.toBytes( apiTraceResult.getApiName() + ":" + apiTraceResult.getTraceStartTime()/1000 + ":" + apiTraceResult.getTraceId()));
                              //其实不推荐关闭WAL，不过关了的确可以提升性能...因为HBase在写数据前会先写WAL，以保证在异常情况下，HBase可以按照WAL的记录来恢复还未持久化的数据。
                                put2.setDurability(Durability.SKIP_WAL);
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("spans"), Bytes.toBytes(JSONObject.toJSONString(apiTraceResult.getSpans())));
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("traceid"), Bytes.toBytes(apiTraceResult.getTraceId()));
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("starttime"), Bytes.toBytes(apiTraceResult.getTraceStartTime()/1000));
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("traceMd5"), Bytes.toBytes(apiTraceResult.getTraceMd5()));
                                put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("duration"), Bytes.toBytes(apiTraceResult.getDuration()));
                                if(StringUtils.isNotEmpty(apiTraceResult.getRegion())){
                                    put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(apiTraceResult.getRegion()));
                                }
                                putList.add(put1);
                                putList.add(put2);
                            }

                            size = putList.size();
                            if(CollectionUtils.isNotEmpty(putList)) {
                            		resultTable.put(putList);
                            }

                        } catch (Exception e) {
                            logger.error(e.getMessage(),e);
                        } finally {
                            if (resultTable != null)
                                resultTable.close();
                        }


                        long end = System.currentTimeMillis();

                        System.out.println(new Date() + "hbase all_trace inset time " + (end - begin) + " size is " + size);

                    }
                });
            }
        });
    }
}
