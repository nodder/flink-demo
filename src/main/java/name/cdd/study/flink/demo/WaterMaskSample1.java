package name.cdd.study.flink.demo;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import com.google.common.collect.Lists;

import name.cdd.study.flink.demo.pojo.CodeTimePair;

//参考：http://blog.csdn.net/lmalds/article/details/52704170
//1461756862000 2016-04-27 19:34:22   窗口（19:34:21 ~ 19:34:24)
//1461756866000 2016-04-27 19:34:26   窗口（19:34:24 ~ 19:34:27)
//1461756872000 2016-04-27 19:34:32   窗口（19:34:20 ~ 19:34:33)
//
//
//000001,1461756862000
//000001,1461756866000
//000001,1461756872000
//000001,1461756873000
//
//000001,1461756874000
//000001,1461756876000
//000001,1461756877000
//000002,1461756879000
//
//000002,1461756871000
//000002,1461756873000
//000002,1461756883000
//
//000002,1461756872000 //有打印。此时水位线为1461756873000,allowlateness为1分钟  (但对于setParallelism为2时，就没有打印。因为有一条水印不满足条件（与的关系？））
//000003,1461756813000 //有打印
//000004,1461756812000 //无打印。 (但对于setParallelism为2时，还是有打印。因为有一条水印减去60秒还满足条件（或的关系？））
public class WaterMaskSample1
{

    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);//TODO setParallelism对于水印的影响分析
        env.getConfig().setAutoWatermarkInterval(9000);//每9秒调用一次getCurrentWatermark。产品代码不需要添加

        DataStream<String> input = env.socketTextStream("localhost", 9000, "\n");
    
        SingleOutputStreamOperator<OutputParams> windowCounts = input
                        .filter(line -> !StringUtils.isNullOrWhitespaceOnly(line))
                        .map(line -> new CodeTimePair(line.split("\\W+")))
                        .assignTimestampsAndWatermarks(new MyPeriodicWatermarks())
                        .keyBy("code")
                        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                        .allowedLateness(Time.minutes(1))//早于水印1分钟（含）之内的数据到达时，仍然处理
                        .apply(new MyWindowFunction());
        
        windowCounts.print().setParallelism(1);
        env.execute("WaterMaskSample1");
    }
    
    private static String timestr(long time)
    {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
    }
    
    @SuppressWarnings ("serial")
    private static class MyPeriodicWatermarks implements AssignerWithPeriodicWatermarks<CodeTimePair>
    {
        private long currentMaxTimestamp = 0L;
        private long maxOutOfOrderness = 10000L;//最大允许乱序时间
        
        private Watermark watermark;
        
        @Override
        public Watermark getCurrentWatermark()
        {
            System.out.println("wall clock is "+ System.currentTimeMillis() +" new watermark "+(currentMaxTimestamp - maxOutOfOrderness));
            this.watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return this.watermark;
        }
        
        @Override
        public long extractTimestamp(CodeTimePair element, long previousElementTimestamp)
        {
            this.currentMaxTimestamp = Math.max(element.getTime(), currentMaxTimestamp);
            System.out.println("timestamp:" + element.getCode() +","+ element.getTime() + "|" + timestr(element.getTime()) 
                              + "," +  currentMaxTimestamp + "|"+ timestr(currentMaxTimestamp) + ","+ (this.watermark == null ? "watermask null" : watermark.toString()));
            return element.getTime();
        }
    }

    
    @SuppressWarnings ("serial")
    private static class MyWindowFunction implements WindowFunction<CodeTimePair, OutputParams, Tuple, TimeWindow>
    {
        public void apply(Tuple key, TimeWindow window, Iterable<CodeTimePair> input, Collector<OutputParams> out) throws Exception
        {
            out.collect(assembleOutputParams(key, window, toList(input)));
        }

        private OutputParams assembleOutputParams(Tuple key, TimeWindow window, List<CodeTimePair> inputList)
        {
            OutputParams output = new OutputParams();
            output.setKey(key);
            output.setInputSize(inputList.size());
            output.setFirstInputTimeStr(timestr(inputList.get(0).getTime()));
            output.setLastInputTimeStr(timestr(inputList.get(inputList.size() - 1).getTime()));
            output.setWindowStartTimeStr(timestr(window.getStart()));
            output.setWindowEndTimeStr(timestr(window.getEnd()));
            return output;
        }

        private static <T> List<T> toList(Iterable<T> it)
        {
            List<T> result = Lists.newArrayList();
            it.forEach(t -> result.add(t));
            return result;
        }
    }

    static class OutputParams//可以使用Tuple6代替
    {
        private Tuple key;
        private long inputSize;
        private String firstInputTimeStr;
        private String lastInputTimeStr;
        private String windowStartTimeStr;
        private String windowEndTimeStr;
        
        public Tuple getKey()
        {
            return key;
        }
        public void setKey(Tuple key)
        {
            this.key = key;
        }
        public long getInputSize()
        {
            return inputSize;
        }
        public void setInputSize(long inputSize)
        {
            this.inputSize = inputSize;
        }
        public String getFirstInputTimeStr()
        {
            return firstInputTimeStr;
        }
        public void setFirstInputTimeStr(String firstInputTimeStr)
        {
            this.firstInputTimeStr = firstInputTimeStr;
        }
        public String getLastInputTimeStr()
        {
            return lastInputTimeStr;
        }
        public void setLastInputTimeStr(String lastInputTimeStr)
        {
            this.lastInputTimeStr = lastInputTimeStr;
        }
        public String getWindowStartTimeStr()
        {
            return windowStartTimeStr;
        }
        public void setWindowStartTimeStr(String windowStartTimeStr)
        {
            this.windowStartTimeStr = windowStartTimeStr;
        }
        public String getWindowEndTimeStr()
        {
            return windowEndTimeStr;
        }
        public void setWindowEndTimeStr(String windowEndTimeStr)
        {
            this.windowEndTimeStr = windowEndTimeStr;
        }
        @Override
        public String toString()
        {
            return "OutputParams [key=" + key + ", inputSize=" + inputSize + ", firstInputTimeStr=" + firstInputTimeStr + ", lastInputTimeStr="
                   + lastInputTimeStr + ", windowStartTimeStr=" + windowStartTimeStr + ", windowEndTimeStr=" + windowEndTimeStr + "]";
        }
    }
}
