package name.cdd.study.flink.demo.sample;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Flink入门第一个例子
 * @author admin
 *
 */
public class SocketWindowWordCount
{
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    private static final long serialVersionUID = 5549609683300213687L;
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
//              .window(TumblingProcessingTimeWindows.of(Time.seconds(3))) //滚动窗口1 可以指定第2个参数offset，用于例如时区。
//              .timeWindow(Time.seconds(3)) //滚动窗口2
//              .window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(3))) //滑动窗口1 可以指定第3个参数offset，用于例如时区。
               .timeWindow(Time.seconds(6), Time.seconds(3)) //滑动窗口2
                .reduce(new ReduceFunction<WordWithCount>() {
                    private static final long serialVersionUID = 4852907855251038442L;

                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
