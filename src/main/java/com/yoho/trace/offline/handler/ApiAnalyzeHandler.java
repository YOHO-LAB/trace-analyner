package com.yoho.trace.offline.handler;

import com.yoho.trace.anaylzer.ApiStatisticsAnalyzer;
import com.yoho.trace.offline.AnalyzeHandleFactory;
import com.yoho.trace.anaylzer.model.ApiTraceResult;
import com.yoho.trace.anaylzer.model.SortedTrace;
import com.yoho.trace.anaylzer.model.SpanInfo;
import com.yoho.trace.anaylzer.model.SpanResult;
import com.yoho.trace.store.ApiStatisticsResultStore;
import com.yoho.trace.utils.MD5;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by xjipeng on 2017/10/12.
 */
public class ApiAnalyzeHandler implements IAnalyzeHandler, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ApiAnalyzeHandler.class) ;

    public void handle(JavaPairRDD<String, SpanInfo> spanInfoPairRDD ){

        ApiStatisticsAnalyzer analyzer = new ApiStatisticsAnalyzer(true) ;

        //按照traceid分组，key为traceid，value为 span list
        JavaPairRDD<String, Iterable<SpanInfo>> tracePairRdd = spanInfoPairRDD.groupByKey() ;

        // <traceid, spanList >  映射为 <traceType, spanList >
        JavaPairRDD<String, SortedTrace> pairRdd = tracePairRdd.flatMapToPair(analyzer.SortSpanTreeFunc) ;

        //map 映射为结果的对象， key为 链条类型的md5
        JavaPairRDD<String, ApiTraceResult> apiTracePairRDD = pairRdd.mapToPair(analyzer.ConvertTraceResultFunc)
                .reduceByKey(analyzer.ReduceFunc);

        //输出，hbase 或者 打印日志
        ApiStatisticsResultStore.store(apiTracePairRDD, "trace_api_analyze");

    }


    /**
     * 为每一种链条生成结果
     * @param tuple2  key为链条类型的md5， value为该类链条的明细list
     * @return   每种链条类型的统计结果， 次数、平均时间、每一步的平均时间、最快和最慢的traceid、
     */
    private Tuple2<String, ApiTraceResult> generateResult(Tuple2<String, Iterable<SortedTrace>> tuple2){

        // calc the times for every trace link
        long duration = 0;
        int times = 0;

        //calc the fastest and slowest trace
        String maxLatencyTrace = new String();
        long maxLatency = 1;
        String minLatencyTrace = new String();
        long minLatency = 9999999;

        //计算出 一种链条中 最快 和 最慢的 traceid，以及 这种链条发生的 总次数、总耗时
        Iterator itor = tuple2._2().iterator();
        while (itor.hasNext()) {

            SortedTrace trace = (SortedTrace) itor.next();
            duration = duration + trace.getDuration();
            times++;

            //slowest
            if (trace.getDuration() > maxLatency) {
                maxLatency = trace.getDuration();
                maxLatencyTrace = trace.getTraceid();
            }

            //fastest
            if (trace.getDuration() < minLatency) {
                minLatency = trace.getDuration();
                minLatencyTrace = trace.getTraceid();
            }
        }

        //计算调用链路中每一步的总耗时
        Map<String, Long> durationPerStep = new HashMap();
        Iterator itor3 = tuple2._2().iterator();
        while (itor3.hasNext()) {
            SortedTrace trace = (SortedTrace) itor3.next();
            List<SpanInfo> spanList = (List<SpanInfo>) trace.getSortSpanList();
            for (int i = 0; i < spanList.size(); i++) {
                String key = spanList.get(i).getName();
                long d = spanList.get(i).getEnd() - spanList.get(i).getBegin();
                if (!durationPerStep.containsKey(key)) {
                    durationPerStep.put(key, d);
                } else {
                    durationPerStep.put(key, durationPerStep.get(key) + d);
                }
            }
        }

        //计算调用链中每一步的平均耗时
        Iterator keyItr = durationPerStep.keySet().iterator();
        while (keyItr.hasNext()) {
            String key = (String) keyItr.next();
            if (durationPerStep.containsKey(key)) {
                durationPerStep.put(key, durationPerStep.get(key) / times);
            }
        }

        //生成调用链路
        Iterator itor2 = tuple2._2().iterator();
        SortedTrace firstSortedTrace = (SortedTrace) itor2.next();

        //call list of trace link
        List<SpanInfo> spanList = firstSortedTrace.getSortSpanList();
        List<SpanResult> list = new ArrayList();
        for (int i = 0; i < spanList.size(); i++) {
            list.add(new SpanResult(spanList.get(i).getName(), durationPerStep.get(String.valueOf(spanList.get(i).getName())),
                    spanList.get(i).getLevel(), spanList.get(i).getSpanid() ,spanList.get(i).getParent() , spanList.get(i).getSrcService(), spanList.get(i).getDstService(), null, null,spanList.get(i).getErrorCount(),spanList.get(i).getSpanType()));
        }


        //设置结果而已
        ApiTraceResult result = new ApiTraceResult();
        result.setApiName(firstSortedTrace.getApi());
        result.setCallTimes(times);
        result.setTraceMd5(tuple2._1());
        result.setSpans(list);
        result.setDuration(duration / times);
        result.setMaxLatencyTrace(maxLatencyTrace);
        result.setMinLatencyTrace(minLatencyTrace);
        result.setMaxLatency((int) maxLatency);
        result.setMinLatency((int) minLatency);

        return new Tuple2(firstSortedTrace.getApi(), result);
    }


    /**
     * 为每个具体的调用链，生成调用链的树形结构，并生成唯一trace link 标识
     * @param spanList
     * @return
     */
    private Tuple2<String, SortedTrace> generateTrace( ArrayList<SpanInfo> spanList ){

        //find root
        Iterator itor = spanList.iterator() ;
        SpanInfo rootSpan = null ;
        while(itor.hasNext() ){
            rootSpan = (SpanInfo)itor.next() ;
            if(rootSpan.getTraceid().equals(rootSpan.getSpanid())){
                break;
            }
        }

        if( rootSpan == null ){
            return null ;
        }

        //sort by time and service name
        Collections.sort(spanList, new Comparator<Object>() {
            public int compare(Object o1, Object o2) {
                if (((SpanInfo) o1).getBegin() < ((SpanInfo) o2).getBegin()) {
                    return -1;
                } else if (((SpanInfo) o1).getBegin() == ((SpanInfo) o2).getBegin()) {
                    if (((SpanInfo) o1).getName().compareToIgnoreCase(((SpanInfo) o2).getName()) <= 0) {
                        return -1;
                    }
                }
                return 1;
            }
        });

        //build tree
        ArrayList<SpanInfo> sortSpanList = new ArrayList<>();
        rootSpan.setLevel(0);
        rootSpan.setParent(rootSpan.getTraceid());
        sortSpanList.add(rootSpan);
        recusive(spanList, rootSpan.getSpanid(), sortSpanList , 0);

        long duration = rootSpan.getEnd() - rootSpan.getBegin() ;
        String api = rootSpan.getName() ;
        StringBuilder key = new StringBuilder() ;
        Iterator it = sortSpanList.iterator() ;
        while(it.hasNext()) {
            key.append( ((SpanInfo)it.next()).getName()+"|");
        }

        String keyMd5 = MD5.md5(key.toString());

        SortedTrace trace = new SortedTrace() ;
        trace.setApi(api);
        trace.setDuration(duration);
        trace.setTraceid(rootSpan.getTraceid());
        trace.setSortSpanList(sortSpanList);

        return new Tuple2<>(keyMd5 , trace );
    }


    /**
     * 递归查找子调用，拼装为树形结构
     * @param spanList
     * @param parentSpanid
     * @param sortSpanList
     * @param count
     */
     private void recusive(ArrayList<SpanInfo> spanList, String parentSpanid, ArrayList<SpanInfo> sortSpanList, int count ){

        if( count > 100 ) return ;

        Iterator itor = spanList.iterator() ;
        while(itor.hasNext()){
            SpanInfo s = (SpanInfo) itor.next() ;
            if( parentSpanid.equals(s.getParent()) && s.getEndpoint().equals("cs") ) {
                s.setLevel(count+1);
                sortSpanList.add(s);
                recusive(spanList, s.getSpanid(), sortSpanList, count+1 ) ;
            }
        }
    }

    private void output(JavaPairRDD<String,ApiTraceResult> apiResultRdd, boolean debugPrint){

        //结果存到hbases中
        ApiStatisticsResultStore.store(apiResultRdd, "trace_api_analyze");

    }





}
