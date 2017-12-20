package name.cdd.study.flink.demo;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.StringUtils;

//evictor
public class TriggerDemo
{

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inStream = env.socketTextStream("localhost", 9000, "\n");
    
        DataStream<Tuple3<String, Integer, String>> result = inStream
                        .filter(line -> !StringUtils.isNullOrWhitespaceOnly(line))
                        .map(line -> {String[] words = line.split("\\W+"); return new Tuple2<>(words[0], 1);})
                        .keyBy(0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(3))) //这个方法中，指定了trigger为ProcessingTimeTrigger，根据处理时间触发，这里每3条处理一次。
                        .trigger(CountTrigger.of(2))
                        .sum(1)
                        .map(t2 -> new Tuple3<>(t2.f0, t2.f1, new SimpleDateFormat("HH:mm:ss.SSS").format(new Date())));
//                        .reduce((a, b) -> new Tuple3<>(a.f0, a.f1 + b.f1, new SimpleDateFormat("HH:mm:ss.SSS").format(new Date())));
        
//        DataStream<Tuple2<String, Integer>> result = inStream
//                        .filter(line -> !StringUtils.isNullOrWhitespaceOnly(line))
//                        .map(line -> {String[] words = line.split("\\W+"); return new Tuple2<>(words[0], 1);})
//                        .keyBy(0)
//                        .countWindow(3)//输入第三个相同的元素时才触发。内部调用了CountTrigger.of(3)
//                        .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
        
        result.print();
        env.execute("KeyedWindow Demo");
    }

}
