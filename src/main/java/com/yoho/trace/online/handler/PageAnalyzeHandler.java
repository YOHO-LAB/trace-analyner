package com.yoho.trace.online.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yoho.trace.anaylzer.model.ApiTraceResult;
import com.yoho.trace.anaylzer.model.SpanInfo;
import com.yoho.trace.sleuth.Span;
import com.yoho.trace.sleuth.Spans;
import com.yoho.trace.store.HBasePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * @Author: lingjie.meng
 * @Descroption:
 * @Date: craete on 下午3:48 in 2017/11/10
 * @ModifyBy:
 */
public class PageAnalyzeHandler implements TraceHandler, Serializable {


	private static Logger logger = LoggerFactory.getLogger(TraceAnalyzeHandler.class);


	private static final String SPLIT_STR = "-";

	private static final String FROM_PAGE = "yoho.fromPage";
	private static final String REGION = "yoho.region";

	@Override
	public void handle(final JavaDStream<Spans> kafkaMsgDStream) {

		// 只取包含yoho.fromPage的span
		JavaDStream<SpanInfo> spanInfoStream = kafkaMsgDStream.flatMap(new FlatMapFunction<Spans, SpanInfo>() {
			@Override
			public Iterator<SpanInfo> call(Spans spans) throws Exception {
				List<SpanInfo> result = Lists.newArrayList();
				List<Span> list = spans.getSpans();
				Iterator<Span> ite = list.iterator();
				SpanInfo spanInfo;
				String pageId;
				String region;
				while (ite.hasNext()) {
					Span span = ite.next();
					//只取包含pageID的span
					pageId = span.tags().get(FROM_PAGE);
					region = span.tags().get(REGION);
					if (StringUtils.isEmpty(pageId) || StringUtils.isEmpty(span.getName()) || span.getName().equals("http:/")) {
						continue;
					}
					//不区分安卓和IOS
					if (pageId.startsWith("iFP") || pageId.startsWith("aFP")) {
						pageId = pageId.substring(1);
					}
					spanInfo = new SpanInfo();
					spanInfo.setPageId(pageId);
					spanInfo.setName(span.getName().replace("http:/",""));
					spanInfo.setBegin(span.getBegin());
					spanInfo.setEnd(span.getEnd());
					spanInfo.setRegion(region);
					result.add(spanInfo);
				}
				return result.iterator();
			}
		});


		// key:pageId:apiname, value ApiTraceResult
		JavaPairDStream<String, ApiTraceResult> pageIdSpanInfoJavaPairDStream = spanInfoStream
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<SpanInfo>, String, ApiTraceResult>() {
					@Override
					public Iterator<Tuple2<String, ApiTraceResult>> call(Iterator<SpanInfo> ite) throws Exception {
						List<Tuple2<String, ApiTraceResult>> list = Lists.newArrayList();
						while (ite.hasNext()) {
							SpanInfo spanInfo = ite.next();
							ApiTraceResult result = new ApiTraceResult();
							result.setDuration(spanInfo.getEnd() - spanInfo.getBegin());
							result.setCallTimes(1);
							list.add(new Tuple2<>(spanInfo.getPageId() + SPLIT_STR + spanInfo.getName(), result));
						}

						return list.iterator();
					}
				});


		JavaPairDStream<String, ApiTraceResult> pageResultStream = pageIdSpanInfoJavaPairDStream.reduceByKey(new Function2<ApiTraceResult, ApiTraceResult, ApiTraceResult>() {
			@Override
			public ApiTraceResult call(ApiTraceResult v1, ApiTraceResult v2) throws Exception {
				ApiTraceResult apiTraceResult = new ApiTraceResult();
				apiTraceResult.setDuration(v1.getDuration() + v2.getDuration());
				apiTraceResult.setCallTimes(v1.getCallTimes() + v2.getCallTimes());
				return apiTraceResult;
			}
		});
		pageResultStream.cache();

		//入库
		pageResultStream.foreachRDD(new VoidFunction<JavaPairRDD<String, ApiTraceResult>>() {
			@Override
			public void call(JavaPairRDD<String, ApiTraceResult> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ApiTraceResult>>>() {
					@Override
					public void call(Iterator<Tuple2<String, ApiTraceResult>> tuple2s) throws Exception {
						long now = Calendar.getInstance().getTimeInMillis();
						try (HTable resultTable1 = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("trace_page_analyze_minutes"));
						     HTable resultTable2 = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("trace_api_source_analyze_minutes"))) {

							List<Put> puts1 = Lists.newArrayList();
							List<Put> puts2 = Lists.newArrayList();

							while (tuple2s.hasNext()) {
								Tuple2<String, ApiTraceResult> tuple2 = tuple2s.next();
								String pageId = tuple2._1.split(SPLIT_STR)[0];
								String api = tuple2._1.split(SPLIT_STR)[1];

								String rowKey1 = pageId + "-" + now + "-" + api;
								logger.info("rowKey is {}", rowKey1);
								Put put1 = new Put(Bytes.toBytes(rowKey1));
								put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("times"), Bytes.toBytes(tuple2._2.getCallTimes()));
								put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("duration"), Bytes.toBytes(tuple2._2.getDuration() / tuple2._2.getCallTimes()));
								put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_duration"), Bytes.toBytes(tuple2._2.getDuration()));
								if(StringUtils.isNotEmpty(tuple2._2.getRegion())){
									put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(tuple2._2.getRegion()));
								}

								puts1.add(put1);
								//logger.info("put data to trace_page_analyze_minutes, {}", put1);


								String rowKey2 = api + "-" + now + "-" + pageId;
								Put put2 = new Put(Bytes.toBytes(rowKey2));
								put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("times"), Bytes.toBytes(tuple2._2.getCallTimes()));
								if(StringUtils.isNotEmpty(tuple2._2.getRegion())){
									put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(tuple2._2.getRegion()));
								}

								puts2.add(put2);
								//logger.info("put data to trace_api_source_analyze_minutes, {}", put2);

							}
							resultTable1.put(puts1);
							resultTable2.put(puts2);
						} catch (Exception e) {
							logger.error("store page result failed, e is {} ", e);
						}
					}
				});

			}
		});


		JavaPairDStream<String, ApiTraceResult> stringTuple2JavaPairDStream = pageResultStream.mapToPair(new PairFunction<Tuple2<String, ApiTraceResult>, String, ApiTraceResult>() {
			@Override
			public Tuple2<String, ApiTraceResult> call(Tuple2<String, ApiTraceResult> tuple2) throws Exception {
				return new Tuple2(tuple2._1.split("-")[0], tuple2._2());
			}
		});

		JavaPairDStream<String, ApiTraceResult> stringApiTraceResultJavaPairDStream = stringTuple2JavaPairDStream.reduceByKey(new Function2<ApiTraceResult, ApiTraceResult, ApiTraceResult>() {
			@Override
			public ApiTraceResult call(ApiTraceResult v1, ApiTraceResult v2) throws Exception {
				return v1.getCallTimes() >= v2.getCallTimes() ? v1 : v2;
			}
		});


		stringApiTraceResultJavaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, ApiTraceResult>>() {
			@Override
			public void call(JavaPairRDD<String, ApiTraceResult> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ApiTraceResult>>>() {
					@Override
					public void call(Iterator<Tuple2<String, ApiTraceResult>> tuple2s) throws Exception {
						long now = Calendar.getInstance().getTimeInMillis();
						try (HTable resultTable1 = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("page_call_time_minutes"))) {
							List<Put> puts1 = Lists.newArrayList();

							while (tuple2s.hasNext()) {
								Tuple2<String, ApiTraceResult> tuple2 = tuple2s.next();
								String rowKey1 = now + "-" + tuple2._1;
								Put put1 = new Put(Bytes.toBytes(rowKey1));
								put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("times"), Bytes.toBytes(tuple2._2.getCallTimes()));
								if(StringUtils.isNotEmpty(tuple2._2.getRegion())){
									put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("region"), Bytes.toBytes(tuple2._2.getRegion()));
								}

								puts1.add(put1);
								logger.info("put data to page_call_time_minutes, {}", put1);
							}
							resultTable1.put(puts1);
						} catch (Exception e) {
							logger.error("store page_call_time_minutes result failed, e is {} ", e);
						}
					}
				});
			}
		});
	}

}
