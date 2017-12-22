package name.cdd.study.flink.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//http://blog.csdn.net/u011239443/article/details/72956515
//https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/table/sql.html
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group

//sql1: in event-time
//输入：
//{"id": 1001, "name":"cdd", "score":80, "occur":"2017/12/18 16:10:00"}
//{"id": 1001, "name":"cdd", "score":100, "occur":"2017/12/18 16:10:02"}
//{"id": 1002, "name":"baly", "score":50, "occur":"2017/12/18 16:10:30"}
//输出：
//3> 1001,180,2017-12-18 08:10:00.0,2017-12-18 08:10:05.0
// kafka输出：{"id":1001,"totalScore":180,"tumbleStart":1513555800000,"tumbleEnd":1513555805000}
//
//
//sql2: in process-time
//测试1：
//    输入：
//    {"id": 1001, "name":"cdd", "score":80, "occur":"2017/12/18 16:10:00"}
//
//    输出：
//    3> 1001,80,2017-12-18 08:31:20.0,2017-12-18 08:31:25.0  //这里的时间是现实时间，但差8小时（需要设置8小时的offset）。虽然代码中设置了是eventtime，但实际上还是按照process时间，到了5秒就输出了，水印好像没起作用。
//
//测试2：
//    输入：
//    {"id": 1001, "name":"cdd", "score":80, "occur":"2017/12/18 16:10:00"}
//    {"id": 1001, "name":"cdd", "score":100, "occur":"2017/12/18 16:10:02"}
//    {"id": 1002, "name":"baly", "score":50, "occur":"2017/12/18 16:10:30"}
//    输出：
//    3> 1001,180,2017-12-18 08:29:40.0,2017-12-18 08:29:45.0
//    4> 1002,50,2017-12-18 08:29:40.0,2017-12-18 08:29:45.0  
public class TableDemo_sql_kafka_with_watermark
{
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
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        DataStream<Tuple4<Integer, String, Integer, Long>> inStreamWithWatermark = env.addSource(new FlinkKafkaConsumer011<>(inputTopic,new JSONDeserializationSchema(),props))
           .map(objNode -> new Tuple4<>(objNode.get("id").asInt(), objNode.get("name").asText(), objNode.get("score").asInt(), toTime(objNode.get("occur").asText())))
           .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, Integer, Long>>(){            
                                                        private static final long serialVersionUID = 604378562837574295L;
                                                        @Override
                                                        public long extractAscendingTimestamp(Tuple4<Integer, String, Integer, Long> element)
                                                        {
                                                            return element.f3;
                                                        }
                                                       
                                                    });
        
        //rowtime使用的是事件事件，proctime使用了处理时间
        tableEnv.registerDataStream("message", inStreamWithWatermark, "id,name,score,customtime,proctime.proctime,rowtime.rowtime");
        
        String sql1 = "select id, sum(score) as totalScore, TUMBLE_START(rowtime, INTERVAL '5' SECOND) as tumbleStart, TUMBLE_END(rowtime, INTERVAL '5' SECOND) as tumbleEnd from message GROUP BY id, TUMBLE(rowtime, INTERVAL '5' SECOND)";
        @SuppressWarnings ("unused")
        String sql2 = "select id, sum(score), TUMBLE_START(proctime, INTERVAL '5' SECOND), TUMBLE_END(proctime, INTERVAL '5' SECOND) from message GROUP BY id, TUMBLE(proctime, INTERVAL '5' SECOND)";
        
        Table resultTable = tableEnv.sqlQuery(sql1);
                        
        DataStream<Row> resultDs = tableEnv.toAppendStream(resultTable, Row.class);
        resultDs.print();
        
        //如果不定义as别名，则输出的key为EXPR$2，EXPR$3等。
        Kafka09JsonTableSink tableSink = new Kafka09JsonTableSink(outputTopic, props, new FlinkFixedPartitioner<Row>());//Kafka09JsonTableSink只能用于append-only的table
        resultTable.writeToSink(tableSink);
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
