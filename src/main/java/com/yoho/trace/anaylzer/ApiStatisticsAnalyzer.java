package com.yoho.trace.anaylzer;

import com.yoho.trace.anaylzer.model.*;
import com.yoho.trace.utils.MD5;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import properties.PropertiesFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by xjipeng on 2017/10/20.
 */
public class ApiStatisticsAnalyzer implements Serializable {

    private static final long serialVersionUID = 1011811836438562696L ;

    private boolean isOffline = false ;

    public ApiStatisticsAnalyzer(boolean isOffline){
        this.isOffline = isOffline ;
    }

    /**
     *  <traceid, spanList>  映射为 <traceType, sortedSpanList>
     */
    public PairFlatMapFunction SortSpanTreeFunc = new PairFlatMapFunction<Tuple2<String, Iterable<SpanInfo>>, String, SortedTrace>() {
        @Override
        public Iterator<Tuple2<String, SortedTrace>> call(Tuple2<String, Iterable<SpanInfo>> tuple2) throws Exception {
            Iterator itor = tuple2._2().iterator();
            ArrayList list = new ArrayList();
            while (itor.hasNext()) {
                list.add(itor.next());
            }
            //key is md5 (trace link type)
            Tuple2<String, SortedTrace> trace = generateTrace(list);
            if (trace != null) {
                return Arrays.asList(trace).iterator();
            }
            return (new ArrayList<Tuple2<String, SortedTrace>>()).iterator();
        }
    } ;


    /**
     * 把 SortedTrace 转换为 ApiTraceResult 对象
     */
    public PairFunction ConvertTraceResultFunc = new PairFunction<Tuple2<String, SortedTrace>, String, ApiTraceResult>() {
        @Override
        public Tuple2<String, ApiTraceResult> call(Tuple2<String, SortedTrace> trace) throws Exception {

            ApiTraceResult result = new ApiTraceResult();

            result.setApiName(trace._2().getApi());
            result.setCallTimes(1);
            result.setTraceMd5(trace._1());
            result.setErrorStatus(trace._2().isErrorStatus());
            result.setTraceId(trace._2().getTraceid());

            List<SpanInfo> spanList = trace._2().getSortSpanList();
            List<SpanResult> spanResultList = new ArrayList<SpanResult>();
            Iterator itor = spanList.iterator();
            while (itor.hasNext()) {
                SpanInfo span = (SpanInfo) itor.next();
                SpanResult spanResult = new SpanResult(span.getName(), span.getEnd() - span.getBegin(), span.getLevel(),
                        span.getSpanid(), span.getParent(), span.getSrcService(), span.getDstService(), null, null,span.getErrorCount(),span.getSpanType());
                spanResultList.add(spanResult);
            }

            result.setSpans(spanResultList);
            result.setDuration(trace._2().getDuration());
            result.setMaxLatencyTrace(trace._2().getTraceid());
            result.setMinLatencyTrace(trace._2().getTraceid());
            result.setMaxLatency((int) trace._2().getDuration());
            result.setMinLatency((int) trace._2().getDuration());
            result.setTraceStartTime(trace._2().getTraceStartTime());
            result.setRestTemplateTimes(trace._2().getRestTemplateTimes());
            result.setRedisTimes(trace._2().getRedisTimes());
            result.setMysqlTimes(trace._2().getMysqlTimes());
            result.setRegion(trace._2().getRegion());
            return new Tuple2<String, ApiTraceResult>(trace._1(), result);
        }
    };


    /**
     * 合并trace
     */
    public Function2 ReduceFunc = new Function2<ApiTraceResult, ApiTraceResult, ApiTraceResult>() {

        //reduce 合并同一个种链条
        @Override
        public ApiTraceResult call(ApiTraceResult v1, ApiTraceResult v2) throws Exception {

            ApiTraceResult result = new ApiTraceResult();
            result.setApiName(v1.getApiName());
            result.setCallTimes(v1.getCallTimes()+v2.getCallTimes());
            result.setTraceMd5(v1.getTraceMd5());
            result.setDuration(  ( v1.getDuration()*v1.getCallTimes() + v2.getDuration()*v2.getCallTimes() ) / result.getCallTimes()  );
            result.setRestTemplateTimes(v1.getRestTemplateTimes() + v2.getRestTemplateTimes());
            result.setRedisTimes(v1.getRedisTimes() + v2.getRedisTimes());
            result.setMysqlTimes(v1.getMysqlTimes() + v2.getMysqlTimes());

            int size = v1.getSpans().size() ;
            for(int i=0; i<size; i++){
                //计算span平均耗时
                long d = (v1.getSpans().get(i).getDuration() * v1.getCallTimes() + v2.getSpans().get(i).getDuration() * v2.getCallTimes() ) / result.getCallTimes() ;
                v1.getSpans().get(i).setDuration(d);

                //计算span异常次数
                int errorCount = v1.getSpans().get(i).getErrorCount() + v2.getSpans().get(i).getErrorCount();
                v1.getSpans().get(i).setErrorCount(errorCount);
            }

            result.setSpans(v1.getSpans());

            if(v1.getMaxLatency() > v2.getMaxLatency() ) {
                result.setMaxLatency(v1.getMaxLatency());
                result.setMaxLatencyTrace(v1.getMaxLatencyTrace());
            }else{
                result.setMaxLatency(v2.getMaxLatency());
                result.setMaxLatencyTrace(v2.getMaxLatencyTrace());
            }

            if(v1.getMinLatency() < v2.getMinLatency() ){
                result.setMinLatency(v1.getMinLatency());
                result.setMinLatencyTrace(v1.getMinLatencyTrace());
            }else{
                result.setMinLatency(v2.getMinLatency());
                result.setMinLatencyTrace(v2.getMinLatencyTrace());
            }

            result.setPrefex(v1.getPrefex());

            return result;
        }
    };


    /**
     * 为每个具体的调用链，生成调用链的树形结构，并生成唯一trace link 标识
     * @param spanList
     * @return
     */
    public Tuple2<String, SortedTrace> generateTrace( ArrayList<SpanInfo> spanList  ){

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
                    return ((SpanInfo) o1).getName().compareToIgnoreCase(((SpanInfo) o2).getName());
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
        //判断该treace是否是异常调用链
        boolean traceErrorStatus = false;
        int restTemplateTimes = 0;
        int mysqlTimes = 0;
        int redisTimes = 0;

        while(it.hasNext()) {
            SpanInfo si = (SpanInfo)it.next();
            key.append(si.getName()+"|");
            if(si.getErrorCount()>0){
                traceErrorStatus = true;
            }
            if(si.getSpanType()== SpanType.RESTTEMPLATE){
                restTemplateTimes++;
            }else if(si.getSpanType()==SpanType.MYSQL){
                mysqlTimes++;
            }else if(si.getSpanType()==SpanType.REDIS){
                redisTimes++;
            }
        }

        String keyMd5 = MD5.md5(key.toString());

        SortedTrace trace = new SortedTrace() ;
        trace.setApi(api);
        trace.setDuration(duration);
        trace.setTraceid(rootSpan.getTraceid());
        trace.setSortSpanList(sortSpanList);
        trace.setErrorStatus(traceErrorStatus);
        //root开始时间当做trace的开始时间
        trace.setTraceStartTime(rootSpan.getBegin());
        trace.setRestTemplateTimes(restTemplateTimes);
        trace.setMysqlTimes(mysqlTimes);
        trace.setRedisTimes(redisTimes);

        //毫秒转换为 秒，丢失 毫秒数
        long beginSec = rootSpan.getReceive()/1000  ;
        //秒 转换为 分钟，丢失 秒数
        long beginMin = beginSec/60 ;
        //转换为小时
        long beginHour = beginMin/60 ;
        //转换为天
        long beginDate = beginHour/24 ;
        //还原为每分钟的开始时间, 单位秒
        trace.setStartMinute(beginMin*60);
        //还原为每天的开始时间，单位秒
        trace.setStartDay(beginDate*24*60*60);

        if( isOffline ){
            //离线分析数据，用天保存
            keyMd5 = trace.getStartDay() + "." + keyMd5 ;
        }else {
            //实时分析数据，用分钟保存
            keyMd5 = trace.getStartMinute() + "." + keyMd5;
        }
        trace.setRegion(rootSpan.getRegion());

        return new Tuple2<>(keyMd5, trace);
    }


    /**
     * 递归查找子调用，拼装为树形结构
     * @param spanList
     * @param parentSpanid
     * @param sortSpanList
     * @param count
     */
    public void recusive(ArrayList<SpanInfo> spanList, String parentSpanid, ArrayList<SpanInfo> sortSpanList, int count ){

        if( count > 100 ) return ;

        Iterator itor = spanList.iterator() ;
        while(itor.hasNext()){
            SpanInfo s = (SpanInfo) itor.next() ;
            //如果spanid==parent 表示这是root，不做处理
            if( parentSpanid.equals(s.getParent()) && s.getEndpoint().equals("cs") && !s.getSpanid().equals(s.getParent())) {
                s.setLevel(count+1);
                sortSpanList.add(s);
                recusive(spanList, s.getSpanid(), sortSpanList, count+1 ) ;
            }
        }
    }




}
