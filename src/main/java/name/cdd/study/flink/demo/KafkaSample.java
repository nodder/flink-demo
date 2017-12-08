package name.cdd.study.flink.demo;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class KafkaSample
{
    private static String bootstrap_servers = "hadoop2:9092,hadoop3:9092,hadoop4:9092";
    private static String group_id = "flink-group";
    
    public static void main(String[] args) throws Exception
    {
          final ParameterTool parameterTool = ParameterTool.fromArgs(args);
          String inputTopic = parameterTool.getRequired("input-topic");
          String outputTopic = parameterTool.getRequired("output-topic");
          String prefix = parameterTool.get("prefix", "PREFIX:");
          
          Properties props = new Properties();
          props.setProperty("bootstrap.servers", bootstrap_servers);
          props.setProperty("group.id", group_id);

          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          
//          env.getConfig().disableSysoutLogging();
          env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
          env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
          env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

          DataStream<String> input = env
                  .addSource(new FlinkKafkaConsumer010<>(//FlinkKafkaConsumer010
                             inputTopic,
                             new SimpleStringSchema(),
                             props))
                  .map(new PrefixingMapper(prefix));

          input.print().setParallelism(1);
//          input.addSink(new FlinkKafkaProducer010<>(
//                        outputTopic,
//                        new SimpleStringSchema(),
//                        props));

          env.execute("Kafka 0.10 Example");
    }
    
    @SuppressWarnings ("serial")
    private static class PrefixingMapper implements MapFunction<String, String> {
        private final String prefix;

        public PrefixingMapper(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String map(String value) throws Exception {
            return prefix + value;
        }
    }
}
