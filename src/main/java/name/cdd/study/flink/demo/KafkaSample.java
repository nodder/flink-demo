package name.cdd.study.flink.demo;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * 最基本的kafka输入输出的例子。
 * @author admin
 *
 */
public class KafkaSample
{
    private static final String SUFFIX = " by cdd.";
    
    /**
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

          /**
           * Creates an execution environment that represents the context in which the program is currently executed. 
           * If the program is invoked standalone, this method returns a local execution environment, as returned by createLocalEnvironment().
           */
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          
          /**
           * Disables the printing of progress update messages to System.out
           */
//          env.getConfig().disableSysoutLogging();
          
          /**
           * Sets the restart strategy to be used for recovery.
           */
          env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
          
          /**
           * Enables checkpointing for the streaming job. The distributed state of the streaming dataflow will be periodically snapshotted. 
           * In case of a failure, the streaming dataflow will be restarted from the latest completed checkpoint. 
           * This method selects CheckpointingMode.EXACTLY_ONCE guarantees.
           */
          env.enableCheckpointing(5000);
          
          /**
           * This method allows users to set custom objects as a global configuration for the job. Since the ExecutionConfig is accessible 
           * in all user defined functions, this is an easy method for making configuration globally available in a job.
           */
          env.getConfig().setGlobalJobParameters(parameterTool);

          /**
           * SimpleStringSchema: Creates a new SimpleStringSchema that uses "UTF-8" as the encoding. One of DeserializationSchema.
           */
          DataStream<String> input = env.addSource(new FlinkKafkaConsumer011<>(inputTopic,new SimpleStringSchema(),props))
                                        .map(new SuffixMapper(SUFFIX));

          input.print().setParallelism(1);
          
          /**
           * SimpleStringSchema : One of SerializationSchema
           */ 
          input.addSink(new FlinkKafkaProducer011<>(
                        outputTopic,
                        new SimpleStringSchema(),
                        props));

          env.execute("Kafka 0.10 Example");
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
