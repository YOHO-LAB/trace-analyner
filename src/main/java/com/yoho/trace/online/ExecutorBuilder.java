package com.yoho.trace.online;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.yoho.trace.online.handler.TraceHandlerStarter;
import com.yoho.trace.sleuth.Spans;

import kafka.serializer.DefaultDecoder;
import properties.PropertiesFactory;
import scala.Tuple2;

/**
 * Created by markeloff on 2017/7/28.
 */
public class ExecutorBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorBuilder.class);
    private static final String APP_NAME = "trace-analyzer";
    private Function0<SparkConf> sparkConf;
    private Function0<JavaStreamingContext> streamingContext;
    private Map<String, String> kafkaParamMap = new HashMap<>();
    private String checkpointDir = "/spark/checkpoint/trace";


    public ExecutorBuilder sparkConf() {
        sparkConf = () -> new SparkConf().setAppName(APP_NAME).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        return this;
    }

    public ExecutorBuilder sparkConf(final String master) {
        this.sparkConf = () -> new SparkConf().setAppName(APP_NAME).setMaster(master);
        return this;
    }

    public ExecutorBuilder javaStreamingContext() {
        assertNotNull(sparkConf, "SparkConf未初始化！！");
        assertNotNull(PropertiesFactory.spark(), "Spark duration未配置！！");
        streamingContext = () -> new JavaStreamingContext(sparkConf.call(),
                Durations.seconds(PropertiesFactory.spark().getDuration()));
        return this;
    }


    public ExecutorBuilder kafkaParamMap() {
        assertNotNull(PropertiesFactory.kafka().getBrokers(), "kafka brokers未配置！！");
        assertNotNull(PropertiesFactory.kafka().getGroup(), "kafka group！！");
        assertNotNull(PropertiesFactory.kafka().getTopic(), "kafka topic未配置！！");
        this.kafkaParamMap = new HashMap<>();
        this.kafkaParamMap.put("bootstrap.servers", PropertiesFactory.kafka().getBrokers());
        this.kafkaParamMap.put("fetch.message.max.bytes", "5242880");
        this.kafkaParamMap.put("group.id", PropertiesFactory.kafka().getGroup());
        return this;
    }


    public ExecutorBuilder checkpointDir(String checkpointDir) {
        this.checkpointDir = checkpointDir;
        return this;

    }

    public JavaStreamingContext build() {
        return JavaStreamingContext.getOrCreate(this.checkpointDir, () -> {
            JavaStreamingContext jsc = this.streamingContext.call();
            HashSet<String> topics = Sets.newHashSet(PropertiesFactory.kafka().getTopic());
            JavaPairInputDStream<byte[], byte[]> kafkaPairInputStream = KafkaUtils.createDirectStream(jsc,
                    byte[].class,
                    byte[].class,
                    DefaultDecoder.class,
                    DefaultDecoder.class,
                    this.kafkaParamMap,
                    topics);
            JavaDStream<Spans> kafkaMsgDStream = kafkaPairInputStream.mapPartitions(msgIterator -> {
                List<Spans> messages = new ArrayList<>();
                ObjectMapper jacksonMapper = new ObjectMapper();

                long current = System.currentTimeMillis() ;

                while (msgIterator.hasNext()) {
                    Tuple2<byte[], byte[]> msgTuple2 = msgIterator.next();
                    byte[] newPayload = extractHeaders(msgTuple2._2());
                    if (logger.isDebugEnabled()) {
                        logger.debug("-------recevie from kafka ===》{}", new String(newPayload));
                    }
                    if (newPayload != null && newPayload.length > 0) {
                        Spans spans = null;
                        try {
                            spans = jacksonMapper.readValue(newPayload, Spans.class);
                            spans.setReceive(current);
                        } catch (Exception e) {
                            logger.error("input spans:{}", e);
                        }
                        if (spans != null && spans.getSpans() != null) {
                            messages.add(spans);
                        }
                    }
                }

                logger.info("kafkaPairInputStream.mapPartitions elapse {} , size {}", System.currentTimeMillis() - current, messages.size());

                return messages.iterator();
            });

            kafkaMsgDStream.cache();

            TraceHandlerStarter.start(kafkaMsgDStream);
            jsc.checkpoint(this.checkpointDir);
            addShutdownHook(jsc);
            return jsc;
        });

    }


    private void assertNotNull(Object ob, String alarm) {
        if (ob == null || "".equals(ob)) throw new RuntimeException(alarm);
    }

    private static void addShutdownHook(final JavaStreamingContext jsc) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("addShutdownHook is called");
                jsc.stop(true, true);
            } catch (Exception e) {
                logger.error("{}", e);
            }
        }));
    }

    private static byte[] extractHeaders(byte[] payload) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
        byteBuffer.get();
        byteBuffer.get();
        byte[] newPayload = new byte[byteBuffer.remaining()];
        byteBuffer.get(newPayload);
        return newPayload;
    }
}
