package com.yoho.trace.online.handler;

import com.yoho.trace.sleuth.Spans;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by markeloff on 2017/7/26.
 */
public class TraceHandlerStarter {
	private static final List<TraceHandler> handlers = new ArrayList();

	static {
		handlers.add(new TraceHbaseHandler());
		handlers.add(new TraceAnalyzeHandler());
		handlers.add(new PageAnalyzeHandler());
	}

	public static void start(final JavaDStream<Spans> kafkaMsgDStream) {
		handlers.forEach(handler -> handler.handle(kafkaMsgDStream));
	}
}
