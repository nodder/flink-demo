package name.cdd.study.flink.demo.sample;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple2;
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

import name.cdd.study.flink.demo.onesight.connectors.redis.RedisHsetRowMapper;
import name.cdd.study.flink.demo.onesight.table.function.GetSumByDay;
import name.cdd.study.flink.demo.onesight.table.function.SumWithinADay;

//https://segmentfault.com/a/1190000005595920
//http://flink.apache.org/news/2016/05/24/stream-sql.html
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//group-id可选
public class TableDemo_tableFunction3_multiSql
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
        
        tableEnv.registerFunction(SumWithinADay.METHOD_NAME, new SumWithinADay());
        tableEnv.registerFunction(GetSumByDay.METHOD_NAME, new GetSumByDay());
        
        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(inputTopic)
                        .withKafkaProperties(props)
                        .withSchema(TableSchema.builder()
                                .field("trans_time", Types.SQL_TIMESTAMP())
                                .field("amount", Types.INT())
                                .field("host", Types.STRING())
                                .field("trans_type", Types.INT())
                                .field("merchant", Types.STRING()).build())
                        .failOnMissingField(false)
                        .build();
        
        tableEnv.registerTableSource("table3", tableSource);
        
//        Table resultTable = tableEnv.sqlQuery("select host, DATE_FORMAT(trans_time, '%Y%m%d') as theDay, sum(amount) from table3 group by host, DATE_FORMAT(trans_time, '%Y%m%d')");//统计每个host,每天的amount之和。

      //{"trans_time":"1514879220497", "amount":111, "host":"192.168.1.1","trans_type":1, "merchant":"Tenpay"}
      //{"trans_time":"1514879223497", "amount":222, "host":"192.168.1.1","trans_type":2, "merchant":"Tenpay"}
      //{"trans_time":"1515043937017", "amount":1000, "host":"192.168.1.1","trans_type":5, "merchant":"Tenpay"} //2018-01-04，其它都是2018-01-02
      //{"trans_time":"1514879223497", "amount":333, "host":"192.168.1.1","trans_type":3, "merchant":"Tenpay"}
        
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(6379).build();
        
        //一个输入，执行多个sql，输出到不同的地方
        exec_sql_retract1(tableEnv, jedisPoolConfig);
        exec_sql_retract2(tableEnv, jedisPoolConfig);
        
        env.execute("");
    }

    private static void exec_sql_retract2(StreamTableEnvironment tableEnv, FlinkJedisPoolConfig jedisPoolConfig)
    {
        Table resultTable = tableEnv.sqlQuery("select host, sum(amount) as host_sum from table3 group by host"); 
        DataStream<Tuple2<Boolean, Row>> resultDs = tableEnv.toRetractStream(resultTable, Row.class);
        
        //过滤掉不需要的false，转换为Row stream
        DataStream<Row> rowResultDs = resultDs.filter(tuple2 -> tuple2.f0)
                .map(tuple2 -> tuple2.f1);
        
        
        // hgetall sum_amount_per_day | hget sum_amount_per_day 20180102
        RedisSink<Row> redisSink = new RedisSink<>(jedisPoolConfig, new RedisHsetRowMapper("table3_2", resultTable.getSchema().getColumnNames(), "host", "host_sum"));
        rowResultDs.addSink(redisSink);
    }

    private static void exec_sql_retract1(StreamTableEnvironment tableEnv, FlinkJedisPoolConfig jedisPoolConfig2)
    {
        //统计每天的amount之和。
        Table resultTable = tableEnv.sqlQuery("select DATE_FORMAT(trans_time, '%Y%m%d') as the_day, sum(amount) as total_amount from table3 group by DATE_FORMAT(trans_time, '%Y%m%d')");
        DataStream<Tuple2<Boolean, Row>> resultDs = tableEnv.toRetractStream(resultTable, Row.class);
        
        //过滤掉不需要的false，转换为Row stream
        DataStream<Row> rowResultDs = resultDs.filter(tuple2 -> tuple2.f0)
                .map(tuple2 -> tuple2.f1);
        
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(6379).build();
        
        // hgetall sum_amount_per_day | hget sum_amount_per_day 20180102
        RedisSink<Row> redisSink = new RedisSink<>(jedisPoolConfig, new RedisHsetRowMapper("table3_1", resultTable.getSchema().getColumnNames(), "the_day", "total_amount"));
        rowResultDs.addSink(redisSink);
    }
}
