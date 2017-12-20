package name.cdd.study.flink.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;


//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//group-id可选
public class CepKafkaDemo
{
    @SuppressWarnings ("serial")
    public static void main(String[] args) throws Exception
    {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputTopic = parameterTool.getRequired("input-topic");
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
        props.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
       KeyedStream<Tuple4<Integer, String, Integer, Long>, Tuple> partitionedInput = env.addSource(new FlinkKafkaConsumer010<>(inputTopic,new JSONDeserializationSchema(),props))
          .map(objNode -> new Tuple4<>(objNode.get("id").asInt(), objNode.get("name").asText(), objNode.get("score").asInt(), toTime(objNode.get("occur").asText())))
          .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, Integer, Long>>(){            
              private static final long serialVersionUID = 604378562837574295L;
              @Override
              public long extractAscendingTimestamp(Tuple4<Integer, String, Integer, Long> element)
              {
                  return element.f3;
              }
             
          })
          .keyBy(0);
       
      Pattern<Tuple4<Integer, String, Integer, Long>, Tuple4<Integer, String, Integer, Long>> pattern = Pattern.<Tuple4<Integer, String, Integer, Long>>begin("start")
                       .next("middle").where(new SimpleCondition<Tuple4<Integer, String, Integer, Long>>() {
                           @Override
                           public boolean filter(Tuple4<Integer, String, Integer, Long> value) throws Exception {
                               System.out.println("next filter:" + value.f1.equals("cdd"));
                               return value.f1.equals("cdd");
                           }
                       })
                       .followedBy("end").where(new SimpleCondition<Tuple4<Integer, String, Integer, Long>>() {
                           @Override
                           public boolean filter(Tuple4<Integer, String, Integer, Long> value) throws Exception {
                               System.out.println("followedBy filter:" + (value.f2 >= 60));
                               return value.f2 >= 60;
                           }
                       })
                       .within(Time.seconds(10));
      
      PatternStream<Tuple4<Integer, String, Integer, Long>> patternStream = CEP.pattern(partitionedInput, pattern);
      
      @SuppressWarnings ("unused")
      SingleOutputStreamOperator<Void> alerts = patternStream.select(new PatternSelectFunction<Tuple4<Integer, String, Integer, Long>, Void>() {

        @Override
        public Void select(Map<String, List<Tuple4<Integer, String, Integer, Long>>> pattern) throws Exception
        {
            System.out.println("in select:" + pattern);
            return null;
        }
      });
        
      env.execute();
    }
    
    private static long toTime(String timeStr)
    {
        try
        {
            return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(timeStr).getTime();
        }
        catch(ParseException e)
        {
            e.printStackTrace();
            return -1;
        }
    }
}
