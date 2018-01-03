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
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import name.cdd.study.flink.demo.onesight.connectors.kafka.KafkaOpenTsdbJsonTableSink;
import name.cdd.study.flink.demo.onesight.table.function.UTCToLocalStr;

//https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/table/sourceSinks.html
//-input-topic fin -output-topic fout -bootstrap-server localhost:9092 -group-id flink-group
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//以kafka topic接收到数据的时间作为row_time。本例每5秒处理一次，那么当kafka接收到数据后，要等到过了这个5秒的周期后，再有接收到新的数据，才会处理这个数据。个人理解，这个就是ingestion_time

public class CommonKafkaSqlMain3_rowtime_kafkaTimestamps
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
                                .field("trans_time", Types.STRING())
                                .field("amount", Types.DOUBLE())
                                .field("host", Types.STRING())
                                .field("trans_type", Types.STRING())
                                .field("is_success", Types.BOOLEAN())
                                .field("card_ass", Types.STRING())
                                .field("merchant", Types.STRING())
                                .field("row_time", Types.SQL_TIMESTAMP())
                                .build())
                        .withKafkaTimestampAsRowtimeAttribute("row_time", new AscendingTimestamps())
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
        
//        {"trans_time":"1514429670429", "amount":129, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//        {"trans_time":"2514429670530", "amount":131, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//        {"trans_time":"3514429670639", "amount":120, "host":"192.168.3.2","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//        {"trans_time":"3514429670639", "amount":11980, "host":"192.168.3.2","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
        Table resultTable = tableEnv.sqlQuery("select host, sum(amount) as totalAmount, UTC_2_LOCAL_STR(TUMBLE_ROWTIME(row_time, INTERVAL '5' SECOND)) as utc_row_time, UTC_2_LOCAL_STR(TUMBLE_START(row_time, INTERVAL '5' SECOND)) as start_time, UTC_2_LOCAL_STR(TUMBLE_END(row_time, INTERVAL '5' SECOND)) as end_time, 'theConst' as const_field from KafkaTable GROUP BY host, TUMBLE(row_time, INTERVAL '5' SECOND)");
        Kafka010JsonTableSink tableSink = new KafkaOpenTsdbJsonTableSink(outputTopic, props, new FlinkFixedPartitioner<Row>(), "zb.mms.trans_type.host.is_success.card_ass.merchant", "utc_row_time", "totalAmount");
        
        resultTable.writeToSink(tableSink);
        
        env.execute("");
    }

}
