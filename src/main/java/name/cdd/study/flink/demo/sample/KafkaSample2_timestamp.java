package name.cdd.study.flink.demo.sample;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

//http://colabug.com/122248.html
//每次输入一个单词，例如aaa
//该例子往output kafka输出时携带了时间戳信息，是0.10x的特性（FlinkKafkaProducer010）。但FlinkKafkaProducer011却没有这种用法，且在1.4中SimpleStringSchema已经被废弃了，改成了另外一个包下的同名文件。
//1.4下，如果有同样的需求，还是再挖掘下更好的写法。
@SuppressWarnings ("deprecation")
public class KafkaSample2_timestamp
{
    private static final String SUFFIX = " by cdd.";
    
    /**
     * 输出部分使用了timestamp
     * args example: -input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
     * -group-id参数可选，其他必选。
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
          final ParameterTool parameterTool = ParameterTool.fromArgs(args);
          String inputTopic = parameterTool.getRequired("input-topic");
          String outputTopic = parameterTool.getRequired("output-topic");
          
          Properties props = new Properties();
          props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
          props.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));

          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
          env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
          env.enableCheckpointing(5000);
          env.getConfig().setGlobalJobParameters(parameterTool);

          DataStream<String> inStream = env.addSource(new FlinkKafkaConsumer011<>(inputTopic,new SimpleStringSchema(),props))
                                        .map(new SuffixMapper(SUFFIX));
          
          DataStream<String> result = inStream
                          .map(word -> new Tuple2<String, Integer>(word, 1))
                          .assignTimestampsAndWatermarks(currentTimeAscendingTimestampExtractor())
                          .keyBy(0)
                          .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                          .allowedLateness(Time.minutes(1))
                          .sum(1)
                          .map(tuple -> tuple.toString());
          
          
          result.print().setParallelism(1);
          FlinkKafkaProducer010Configuration<String> producerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
              result,                   // input stream
              outputTopic,                 // target topic
              new SimpleStringSchema(),   // serialization schema
              props);                // custom configuration for KafkaProducer (including broker list)

          // the following is necessary for at-least-once delivery guarantee
          /**
           * 如果true，发送kafka失败时，只是log，不会中断执行，这样可能丢数据
           * 如果false，发送kafka失败时，抛异常，这样job会restart，不会丢数据，但是会中断执行；这里最好把producer的retires设成3，这样避免Kafka临时不可用导致job中断，比如leader切换
           * 默认为false
           */
          producerConfig.setLogFailuresOnly(false); 
          
          /**
           * 如果true，在做checkpoint的时候，会等待所有pending的record被发送成功，这样保证数据不丢。
           * 默认为false
           */
          producerConfig.setFlushOnCheckpoint(true); 
          producerConfig.setWriteTimestampToKafka(true);
      
          env.execute("KafkaSample2 Timestamp Example");
    }

    @SuppressWarnings ("serial")
    private static AscendingTimestampExtractor<Tuple2<String, Integer>> currentTimeAscendingTimestampExtractor()
    {
        return new AscendingTimestampExtractor<Tuple2<String, Integer>>() {
                                              @Override
                                              public long extractAscendingTimestamp(Tuple2<String, Integer> element) {
                                                  return System.currentTimeMillis();
                                              }
                                         };
    }
    
    @SuppressWarnings ("serial")
    private static class SuffixMapper implements MapFunction<String, String> {
        private final String suffix;

        public SuffixMapper(String prefix) {
            this.suffix = prefix;
        }

        @Override
        public String map(String value) throws Exception {
            return value + suffix;
        }
    }
}
