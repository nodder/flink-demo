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

//https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/table/sourceSinks.html
//-input-topic fin -output-topic fout -bootstrap-server localhost:9092 -group-id flink-group
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//{"trans_time":"1514879220497", "amount":120, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//{"trans_time":"1514879220999", "amount":140, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//{"trans_time":"1514879223497", "amount":240, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//{"trans_time":"1514879225000", "amount":100, "host":"192.168.3.2","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//{"trans_time":"1514879254555", "amount":1000, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//{"trans_time":"1514879255000", "amount":1000, "host":"192.168.2.1","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}//输出amount500
public class CommonKafkaSqlMain4_rowtime
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
                            // 计算水位线时根据当前时间减去30秒。同WaterMaskSample1中的maxOutOfOrderness，而不是allowedLateness，后者在本类中为0且不能设置。
                            new BoundedOutOfOrderTimestamps(30000L))
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
        
        Table resultTable = tableEnv.sqlQuery("select host, sum(amount) as totalAmount, TUMBLE_ROWTIME(trans_time, INTERVAL '5' SECOND) as row_time, TUMBLE_START(trans_time, INTERVAL '5' SECOND) as start_time, TUMBLE_END(trans_time, INTERVAL '5' SECOND) as end_time, 'theConst' as const_field from KafkaTable GROUP BY host, TUMBLE(trans_time, INTERVAL '5' SECOND)");
        Kafka010JsonTableSink tableSink = new KafkaOpenTsdbJsonTableSink(outputTopic, props, new FlinkFixedPartitioner<Row>(), "zb.mms.trans_type.host.is_success.card_ass.merchant", "row_time", "totalAmount");
        
        resultTable.writeToSink(tableSink);
        
        env.execute("");
    }

}
