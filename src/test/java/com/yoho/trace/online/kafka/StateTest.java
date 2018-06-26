package com.yoho.trace.online.kafka;

import com.google.common.collect.Sets;
import kafka.serializer.StringDecoder;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import properties.PropertiesFactory;
import scala.Tuple2;

import java.util.*;

/**
 * Created by xjipeng on 2017/10/23.
 */
public class StateTest {

    public static void main(String args[]){


        HashMap kafkaParamMap = new HashMap<>();
        kafkaParamMap.put("bootstrap.servers", "server1:9092,server2:9092,server3:9092" );
        kafkaParamMap.put("fetch.message.max.bytes", "104857600");
        kafkaParamMap.put("group.id", "wordstate");

        HashSet<String> topics = Sets.newHashSet("TEST-TOPIC-WORDS") ;

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("StateTest");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaPairInputDStream<String, String> kafkaPairInputStream = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParamMap,
                topics);

        JavaPairDStream<String, Integer > pairGroup = kafkaPairInputStream.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {

                List<Tuple2<String, Integer>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> tuple2 = tuple2Iterator.next();

                    String[] words = String.valueOf(tuple2._2()).split(" ");
                    for (int i = 0; i < words.length; i++) {
                        list.add(new Tuple2<>(words[i], 1));
                    }
                }

                return list.iterator();
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }) ;


        JavaPairDStream<String,Integer> pairDStream  = pairGroup.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String,Integer> >() {
            @Override
            public Tuple2<String,Integer> call(String key, Optional<Integer> newValue, State<Integer> stateValue) throws Exception {

                int count = 0 ;
                if( stateValue.exists() ){
                    count = stateValue.get() ;
                }

                count = count + newValue.orElse(0) ;

                stateValue.update(count);

                System.out.println("state updated, key:" + key + " count:" + count);

                return new Tuple2<String, Integer>(key,count);
            }
        }).timeout(Durations.seconds(600000))).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Iterator<Tuple2<String, Integer>> t2) throws Exception {

                List<Tuple2<String, Integer>> list = new ArrayList();
                while(t2.hasNext()){
                    Tuple2<String, Integer> tt = t2.next() ;
                    list.add(new Tuple2<String, Integer>(tt._1(), tt._2()));
                }
                return list.iterator();
            }
        }) ;

//
//
//        JavaPairDStream<String,Integer> pairDStream = pairGroup.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String,Integer> >() {
//            @Override
//            public Tuple2<String,Integer> call(String key, Optional<Integer> newValue, State<Integer> stateValue) throws Exception {
//
//                int count = 0 ;
//                if( stateValue.exists() ){
//                    count = stateValue.get() ;
//                }
//
//                count = count + newValue.orElse(0) ;
//
//                stateValue.update(count);
//
//                System.out.println("state updated, key:" + key + " count:" + count);
//
//                return new Tuple2<String, Integer>(key,count);
//            }
//        }).timeout(Durations.seconds(600000))).stateSnapshots() ;


        pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIterableJavaPairRDD) throws Exception {
                stringIterableJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        while(tuple2Iterator.hasNext()){
                            Tuple2<String, Integer> t2 = tuple2Iterator.next() ;
                            System.out.println("------------ key:" + t2._1() + " value:" + t2._2());
                        }
                    }
                });
            }
        });


        jssc.checkpoint("/Users/xjipeng/code/spark/checkpoint5");

        jssc.start();

        try {
            jssc.awaitTermination();
        }catch (Exception ex){

        }

    }


}
