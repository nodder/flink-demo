package name.cdd.study.flink.demo.onesight.perf;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import name.cdd.study.flink.demo.onesight.connectors.kafka.FlinkRandomPartitioner;
import name.cdd.study.flink.demo.onesight.connectors.redis.RedisSetRowMapper;
import name.cdd.study.flink.demo.onesight.table.function.UTCToLocalStr;

//1 : FlinkFixedPartitioner
//-input-topic fin3 -output-topic fout3 -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group -partitionType 1

//2 : FlinkRandomPartitioner
//-input-topic fin3 -output-topic fout3 -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group -partitionType 2
//配合kafka批量发送工具的配置：5000000 | {"uuid":"test-uuid-uuid-uuid-[[1,+1]]"}
//滚动窗口在分组数量大时的运行情况
public class OutOfMemJob
{    
    public static void main(String[] args) throws Exception
    {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputTopic = parameterTool.getRequired("input-topic");
        String outputTopic = parameterTool.getRequired("output-topic");
        int partitionType = parameterTool.getInt("partitionType");
//        int parallelism = parameterTool.getInt("parallelism");
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
        props.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(parallelism);
        
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        tableEnv.registerFunction(UTCToLocalStr.METHOD_NAME, new UTCToLocalStr());

        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(inputTopic)
                        .withKafkaProperties(props)
                        .withSchema(TableSchema.builder()
                                .field("uuid", Types.STRING())
                                .field("proccess_time", Types.SQL_TIMESTAMP())
                                .build())
                        .withProctimeAttribute("proccess_time")
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
//        Table resultTable = tableEnv.sqlQuery("select uuid, proccess_time , 'theConst' as const_field from KafkaTable");
        Table resultTable = tableEnv.sqlQuery("select uuid, UTC_2_LOCAL_STR(HOP_PROCTIME(proccess_time, INTERVAL '5' SECOND, INTERVAL '2' MINUTE)) as utc_proc_time, UTC_2_LOCAL_STR(HOP_START(proccess_time, INTERVAL '5' SECOND, INTERVAL '2' MINUTE)) as start_time, UTC_2_LOCAL_STR(HOP_END(proccess_time, INTERVAL '5' SECOND, INTERVAL '2' MINUTE)) as end_time, 'theConst' as const_field from KafkaTable GROUP BY uuid, HOP(proccess_time, INTERVAL '5' SECOND, INTERVAL '2' MINUTE)");
        sinkToKafka(outputTopic, props, resultTable, partitionType);
//        sinkToRedis(tableEnv, resultTable);

        env.execute("OutOfMemJob");
    }

    private static void sinkToKafka(String outputTopic, Properties props, Table resultTable, int partitionType)
    {
        FlinkKafkaPartitioner<Row> partitioner = getPartitoner(partitionType);
        Kafka010JsonTableSink tableSink = new Kafka010JsonTableSink(outputTopic, props, partitioner);
        resultTable.writeToSink(tableSink);
    }

    private static FlinkKafkaPartitioner<Row> getPartitoner(int partitionType)
    {
        switch(partitionType)
        {
            case 1:
                System.out.println("using FlinkFixedPartitioner");
                return new FlinkFixedPartitioner<Row>();
            case 2:
                System.out.println("using FlinkRandomPartitioner");
                return new FlinkRandomPartitioner<>();
            default:
                System.out.println("error, unknown partitionType:" + partitionType);
                return null;
        }
    }

    static void sinkToRedis(StreamTableEnvironment tableEnv, Table resultTable)
    {
        DataStream<Row> resultDs = tableEnv.toDataStream(resultTable, Row.class);
        
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(6379).build();
        
        RedisSink<Row> redisSink = new RedisSink<>(jedisPoolConfig, new RedisSetRowMapper("myset", resultTable.getSchema().getColumnNames()));
        resultDs.addSink(redisSink);
    }
}
