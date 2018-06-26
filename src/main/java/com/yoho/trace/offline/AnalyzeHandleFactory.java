package com.yoho.trace.offline;

import com.yoho.trace.offline.handler.ApiAnalyzeHandler;
import com.yoho.trace.anaylzer.model.SpanInfo;
import com.yoho.trace.offline.handler.IAnalyzeHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.PropertiesFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by xjipeng on 2017/10/12.
 */
public class AnalyzeHandleFactory {

    private static List<IAnalyzeHandler> analyzeHandlers;

    private static final Logger logger = LoggerFactory.getLogger(AnalyzeHandleFactory.class) ;

    static {
        analyzeHandlers = new ArrayList<>();
        analyzeHandlers.add(new ApiAnalyzeHandler()) ;
        //other
        //
    }


    public static void start(JavaSparkContext jsc){

        //load trace link from hbase
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseTraceRDD = load(jsc) ;

        hBaseTraceRDD.cache() ;
        //logger.info("load data finish, count {}", hBaseTraceRDD.count());

        //key is traceid ,value is span info
        JavaPairRDD<String, SpanInfo> hBaseTracePairRDD = hBaseTraceRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, SpanInfo>() {
            @Override
            public Tuple2<String, SpanInfo> call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                return new Tuple2<String, SpanInfo>(
                        Bytes.toString(tuple._2().getValue(Bytes.toBytes("span"), Bytes.toBytes("traceid"))), parseSpanInfo(tuple._2())
                );
            }
        }) ;

        Iterator itor = analyzeHandlers.iterator() ;
        while(itor.hasNext()){
            ((IAnalyzeHandler)itor.next()).handle(hBaseTracePairRDD);
        }

    }


    /**
     *
     * @param jsc
     * @return
     */
    private static JavaPairRDD<ImmutableBytesWritable, Result> load(JavaSparkContext jsc){

        logger.info("begin load data");

        try {

            Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", PropertiesFactory.hbase().getZkquorum() );
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
            hbaseConf.set("hbase.defaults.for.version.skip", "true");
            hbaseConf.set(TableInputFormat.INPUT_TABLE, "trace");


            long current = System.currentTimeMillis()/1000 ;
            long day = current/3600/24 ;
            long dayBegin = day*24*3600 - 8*3600;

            Scan scan = new Scan();
            //只扫描今天的
            scan.setStartRow(Bytes.toBytes(String.valueOf(dayBegin))) ;
            scan.setStopRow(Bytes.toBytes(String.valueOf(current)));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("name"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("spanid"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("traceid"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("begin"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("end"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("parent"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("service"));
            scan.addColumn(Bytes.toBytes("span"), Bytes.toBytes("ip"));

            //analyze call trace by time range
            //scan.setTimeRange()

            scan.setCaching(10000);
            scan.setCacheBlocks(false);

            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            hbaseConf.set(TableInputFormat.SCAN, ScanToString);

            HBaseAdmin.checkHBaseAvailable(hbaseConf);

            return jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        }catch (Exception e){
            e.printStackTrace();
        }

        return null ;
    }


    public static void main(String args[]){
        long current = System.currentTimeMillis()/1000 ;
        long day = current/3600/24 ;
        long dayBegin = day*24*3600 - 8*3600;

        System.out.println("current:" + current + " day begin:" + dayBegin );

    }


    /**
     *
     * @param result
     * @return
     */
    static SpanInfo parseSpanInfo(Result result) {

        SpanInfo span = new SpanInfo();

        span.setName(Bytes.toString(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("name"))));
        span.setTraceid(Bytes.toString(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("traceid"))));
        span.setSpanid(Bytes.toString(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("spanid"))));
        span.setParent(Bytes.toString(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("parent"))));
        span.setBegin(Bytes.toLong(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("begin"))));
        span.setEnd(Bytes.toLong(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("end"))));
        span.setEndpoint(Bytes.toString(result.getRow()).split(":")[3]);

        span.setService(Bytes.toString(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("service"))));
        span.setIp(Bytes.toString(result.getValue(Bytes.toBytes("span"), Bytes.toBytes("ip"))));

        return span ;
    }



}



