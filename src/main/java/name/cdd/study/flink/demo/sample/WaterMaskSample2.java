package name.cdd.study.flink.demo.sample;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.StringUtils;

//http://www.cnblogs.com/dongxiao-yang/p/7610412.html
//aaaa 1506590035000
//cc 1506590035000
//cc 1506590035000
//bb 1506590035000
//aaaa 1506590035000
//bb 1506590035000
//
//aaaa 1506590041000
//bb 1506590041000
//cc 1506590041000
//
//aaaa 1506590051000 //setParallelism(1)时，这条就有打印了
//bb 1506590051000  //setParallelism(2)时，这条有打印
//cc 1506590051000 
//
//自己添加 
//cc 1506590000000 //打印(cc,1506590000000,1)
//cc 1506590000000  //打印(cc,1506590000000,2)
public class WaterMaskSample2 {

    public static void main(String[] args) throws Exception {
        String hostName = "localhost";
        int port = 9000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);//注意！setParallelism(2)
        env.getConfig().setAutoWatermarkInterval(9000);

        DataStream<Tuple3<String, Long, Integer>> result = env.socketTextStream(hostName, port)
                  .filter(line -> !StringUtils.isNullOrWhitespaceOnly(line))
                  .map(line -> {String[] tokens = line.toLowerCase().split("\\W+"); return new Tuple3<>(tokens[0], Long.parseLong(tokens[1]), 1);})
                  .assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks())
                  .keyBy(0)
                  .timeWindow(Time.seconds(20))
                  .allowedLateness(Time.seconds(60))
                  .sum(2);

        result.print();

        env.execute("WaterMaskSample2");
    }
    
    @SuppressWarnings ("serial")
    private static class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>
    {
        private final long maxOutOfOrderness = 10000L;
        private long currentMaxTimestamp = 0L;
        
        @Override
        public Watermark getCurrentWatermark() {
            System.out.println("wall clock is "+ System.currentTimeMillis() +" new watermark "+(currentMaxTimestamp - maxOutOfOrderness));
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
        
        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
            long timestamp= element.f1;
            this.currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("get timestamp is "+timestamp+" currentMaxTimestamp "+currentMaxTimestamp);
            return timestamp;
        }
    }
}
