package name.cdd.study.flink.demo.onesight;


import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import name.cdd.study.flink.demo.onesight.connectors.kafka.KafkaOpenTsdbJsonTableSink;
import name.cdd.study.flink.demo.onesight.table.function.UTCToLocalStr;

//-input-topic fin -output-topic fout -bootstrap-server localhost:9092 -group-id flink-group
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//商发部的需求：按照卡组织分组，统计5分钟窗口的交易量、成功率等
public class CommonKafkaSqlMain5_rowtime_statistic_sql
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
        
        tableEnv.registerFunction(UTCToLocalStr.METHOD_NAME, new UTCToLocalStr());
        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(inputTopic)
                        .withKafkaProperties(props)
                        .withSchema(TableSchema.builder()
                                .field("trans_time", Types.SQL_TIMESTAMP())
                                .field("amount", Types.DOUBLE())
                                .field("host", Types.STRING())
                                .field("trans_type", Types.STRING())
                                .field("is_success", Types.BOOLEAN())
                                .field("card_ass", Types.STRING())
                                .field("merchant", Types.STRING())
                                .build())
                        .withRowtimeAttribute("trans_time", 
                            // value of "trans_time" is extracted from existing field with same name
                            new ExistingField("trans_time"),
                            // values of "row_time" are at most out-of-order by 30 seconds
                            new BoundedOutOfOrderTimestamps(0L))
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
        
        String sql1 = "select card_ass, sum(amount) as totalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time, TUMBLE_START(trans_time, INTERVAL '5' SECOND) as start_time, TUMBLE_END(trans_time, INTERVAL '5' SECOND) as end_time from KafkaTable GROUP BY card_ass, TUMBLE(trans_time, INTERVAL '5' SECOND)";
        String sql2 = "select card_ass, sum(amount) as totalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time, TUMBLE_START(trans_time, INTERVAL '5' SECOND) as start_time from KafkaTable GROUP BY card_ass, is_success, TUMBLE(trans_time, INTERVAL '5' SECOND) HAVING is_success=true";
        
        String sql10 = "select a.card_ass, a.totalAmount, a.row_time, a.start_time, a.end_time, b.sucessTotalAmount from (select card_ass, sum(amount) as totalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time, TUMBLE_START(trans_time, INTERVAL '5' SECOND) as start_time, TUMBLE_END(trans_time, INTERVAL '5' SECOND) as end_time from KafkaTable GROUP BY card_ass, TUMBLE(trans_time, INTERVAL '5' SECOND)) as a, (select card_ass, sum(amount) as sucessTotalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time from KafkaTable GROUP BY card_ass, is_success, TUMBLE(trans_time, INTERVAL '5' SECOND) HAVING is_success=true) as b where a.card_ass=b.card_ass and a.row_time = b.row_time";
        String sql11 = "select a.card_ass, a.totalAmount, a.row_time, a.start_time, a.end_time, b.sucessTotalAmount, b.success_trans_count, a.trans_count, (b.success_trans_count*100/a.trans_count) as success_rate from (select card_ass, sum(amount) as totalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time, TUMBLE_START(trans_time, INTERVAL '5' SECOND) as start_time, TUMBLE_END(trans_time, INTERVAL '5' SECOND) as end_time, count(card_ass) as trans_count from KafkaTable GROUP BY card_ass, TUMBLE(trans_time, INTERVAL '5' SECOND)) as a, (select card_ass, sum(amount) as sucessTotalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time, count(card_ass) as success_trans_count from KafkaTable GROUP BY card_ass, is_success, TUMBLE(trans_time, INTERVAL '5' SECOND) HAVING is_success=true) as b where a.card_ass=b.card_ass and a.row_time = b.row_time";
        
        Table resultTable = tableEnv.sqlQuery(sql11);
        Kafka010JsonTableSink tableSink = new KafkaOpenTsdbJsonTableSink(outputTopic, props, new FlinkFixedPartitioner<Row>(), "zb.mms.trans_type.host.is_success.card_ass.merchant", "row_time", "totalAmount");
        
        resultTable.writeToSink(tableSink);
        
        env.execute("");
    }

}
