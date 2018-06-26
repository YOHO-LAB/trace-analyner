package com.yoho.trace.online.handler;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yoho.trace.sleuth.Spans;
import com.yoho.trace.store.HBasePool;
import com.yoho.trace.store.TraceSpanStore;

import scala.util.Random;

/**
 * Created by markeloff on 2017/7/26.
 */
public class TraceHbaseHandler implements TraceHandler, Serializable {

	Logger logger = LoggerFactory.getLogger(TraceHbaseHandler.class);

	@Override
	public void handle(final JavaDStream<Spans> kafkaMsgDStream) {

		kafkaMsgDStream.foreachRDD(new VoidFunction<JavaRDD<Spans>>() {
			@Override
			public void call(JavaRDD<Spans> spansJavaRDD) throws Exception {
				spansJavaRDD.foreachPartition(new VoidFunction<Iterator<Spans>>() {
					@Override
					public void call(Iterator<Spans> spansIterator) throws Exception {
						// HTable traceTable = null ;
						HTable[] tables = new HTable[3];
						int count = 0;
						long begin = System.currentTimeMillis();
						try {
							for (int i = 0; i < 3; i++) {
								tables[i] = (HTable) HBasePool.getConnection().getTable(TableName.valueOf("trace"));
								tables[i].setWriteBufferSize(1024 * 1024 * 20);
								tables[i].setAutoFlush(false, true);
								logger.info("flush spans to hbase, count {}, elapse {}", count,System.currentTimeMillis() - begin);
							}
							while (spansIterator.hasNext()) {
								int random=new Random().nextInt(3);
								count = count + TraceSpanStore.store(spansIterator.next(), tables[random]);
							}
							for (int i = 0; i < 3; i++) {
								tables[i].flushCommits();
							}

						} finally {
							try {
								for (HTable hTable : tables) {
									if (hTable != null)
										hTable.close();
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

						long end = System.currentTimeMillis();

						System.out.println(new Date() + "hbase trace inset time " + (end - begin) + " size is " + count);

					}
				});
			}
		});
	}
}
