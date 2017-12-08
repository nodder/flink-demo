package name.cdd.study.flink.demo;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

import java.util.stream.Stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import name.cdd.study.flink.demo.pojo.WordWithCount;

public class SocketWindowWordCountJava8
{
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
    
        DataStream<WordWithCount> windowCounts = text
                        .flatMap((String value, Collector<WordWithCount> out) -> Stream.of(value.split("\\s")).forEach(word -> out.collect(new WordWithCount(word, 1L))))
                        .keyBy("word")
                        .timeWindow(seconds(6), seconds(3)) //timeWindow(Time.seconds(3))
                        .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count));
        
        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
}
