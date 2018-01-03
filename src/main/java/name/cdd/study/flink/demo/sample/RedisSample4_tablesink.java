package name.cdd.study.flink.demo.sample;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import name.cdd.study.flink.demo.onesight.connectors.redis.RedisSetRowMapper;
import name.cdd.study.flink.demo.onesight.table.function.UTCToLocalStr;

//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
public class RedisSample4_tablesink
{
    public static void main(String[] args) throws Exception
    {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputTopic = parameterTool.getRequired("input-topic");
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
        props.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        tableEnv.registerFunction(UTCToLocalStr.METHOD_NAME, new UTCToLocalStr());

        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(inputTopic)
                        .withKafkaProperties(props)
                        .withSchema(TableSchema.builder()
                                .field("trans_time", Types.STRING())
                                .field("amount", Types.DOUBLE())
                                .field("host", Types.STRING())
                                .field("trans_type", Types.STRING())
                                .field("is_success", Types.BOOLEAN())
                                .field("card_ass", Types.STRING())
                                .field("merchant", Types.STRING())
                                .field("proccess_time", Types.SQL_TIMESTAMP())
                                .build())
//                        .failOnMissingField(false)//默认值也是false
                        .withProctimeAttribute("proccess_time")
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
        
//        {"trans_time":"1514429670429", "amount":129, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//        {"trans_time":"2514429670530", "amount":131, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//        {"trans_time":"3514429670639", "amount":120, "host":"192.168.3.2","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//        {"trans_time":"3514429670639", "amount":11980, "host":"192.168.3.2","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
        Table resultTable = tableEnv.sqlQuery("select host, sum(amount) as totalAmount, UTC_2_LOCAL_STR(TUMBLE_PROCTIME(proccess_time, INTERVAL '5' SECOND)) as utc_proc_time, UTC_2_LOCAL_STR(TUMBLE_START(proccess_time, INTERVAL '5' SECOND)) as start_time, UTC_2_LOCAL_STR(TUMBLE_END(proccess_time, INTERVAL '5' SECOND)) as end_time, 'theConst' as const_field from KafkaTable GROUP BY host, TUMBLE(proccess_time, INTERVAL '5' SECOND)");
       
        DataStream<Row> resultDs = tableEnv.toDataStream(resultTable, Row.class);
        
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(6379).build();
        
//        RedisSink<Row> redisSink = new RedisSink<>(jedisPoolConfig, new RedisPublishRowMapper("cdd", resultTable.getSchema().getColumnNames()));
        RedisSink<Row> redisSink = new RedisSink<>(jedisPoolConfig, new RedisSetRowMapper("myset", resultTable.getSchema().getColumnNames()));

        resultDs.addSink(redisSink);

        env.execute("");
    }
}
