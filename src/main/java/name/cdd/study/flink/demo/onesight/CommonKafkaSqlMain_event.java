package name.cdd.study.flink.demo.onesight;


import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
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
import org.apache.flink.types.Row;

import name.cdd.study.flink.demo.onesight.connectors.kafka.KafkaOpenTsdbJsonTableSink;
import name.cdd.study.flink.demo.onesight.table.function.UTCToLocal;

//-input-topic fin -output-topic fout -bootstrap-server localhost:9092 -group-id flink-group
public class CommonKafkaSqlMain_event
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
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        tableEnv.registerFunction(UTCToLocal.METHOD_NAME, new UTCToLocal());

        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(inputTopic)
                        .withKafkaProperties(props)
                        .withSchema(TableSchema.builder()
                                .field("trans_time", Types.STRING())
                                .field("amount", Types.STRING())
                                .field("host", Types.STRING())
                                .field("trans_type", Types.STRING())
                                .field("is_success", Types.BOOLEAN())
                                .field("card_ass", Types.STRING())
                                .field("merchant", Types.STRING())
                                .field("proccess_time", Types.SQL_TIMESTAMP())
                                .build())
                        .failOnMissingField(false)
//                        .withKafkaTimestampAsRowtimeAttribute(rowtimeAttribute, watermarkStrategy)
                        
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
        
        Table resultTable = tableEnv.sqlQuery("select trans_time, amount, host, trans_type, is_success, card_ass, merchant, UTC_2_LOCAL(proccess_time) as procc_time from KafkaTable");
                
        Kafka010JsonTableSink tableSink = new KafkaOpenTsdbJsonTableSink(outputTopic, props, new FlinkFixedPartitioner<Row>(), "zb.mms.trans_type.host.is_success.card_ass.merchant", "trans_time", "amount");
        resultTable.writeToSink(tableSink);
        
        env.execute("CommonKafkaSqlMain");
    }

}
