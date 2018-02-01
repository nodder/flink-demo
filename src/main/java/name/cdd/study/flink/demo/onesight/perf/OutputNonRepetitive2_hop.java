package name.cdd.study.flink.demo.onesight.perf;

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

import name.cdd.study.flink.demo.onesight.table.function.DiffFunction;
import name.cdd.study.flink.demo.onesight.table.function.UTCToLocalStr;

//-input-topic fin3 -output-topic fout3 -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group2
//滑动窗口，统计count，只输出变化的内容。
//{"trans_time":"1514429670429", "amount":100, "custom_id":"001","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
//{"trans_time":"1514429670429", "amount":200, "custom_id":"002","trans_type":"2","is_success":true, "card_ass":"DOMN", "merchant":"Tenpay"}
public class OutputNonRepetitive2_hop
{
    public static void main(String[] args) throws Exception
    {
        final ParameterTool pt = ParameterTool.fromArgs(args);
        Properties kafkaProps = getKafkaPropsFromInput(pt);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        tableEnv.registerFunction(UTCToLocalStr.METHOD_NAME, new UTCToLocalStr());
        tableEnv.registerFunction(DiffFunction.METHOD_NAME, new DiffFunction());
        
        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(fromInput(pt, "input-topic"))
                        .withKafkaProperties(kafkaProps)
                        .withSchema(TableSchema.builder()
                                .field("trans_time", Types.SQL_TIMESTAMP())
                                .field("amount", Types.DOUBLE())
                                .field("custom_id", Types.STRING())
                                .field("trans_type", Types.STRING())
                                .field("is_success", Types.BOOLEAN())
                                .field("card_ass", Types.STRING())
                                .field("merchant", Types.STRING())
                                .field("proctime", Types.SQL_TIMESTAMP())
                                .build())
                        .withProctimeAttribute("proctime")
                        .build();

        tableEnv.registerTableSource("KafkaTable", tableSource);
        
        String sql = "select custom_id, "
                          + "count(custom_id) as custom_count, "
                          + "UTC_2_LOCAL_STR(HOP_PROCTIME(proctime, INTERVAL '5' SECOND, INTERVAL '20' SECOND)) as utc_proc_time, "
                          + "UTC_2_LOCAL_STR(HOP_START(proctime, INTERVAL '5' SECOND, INTERVAL '20' SECOND)) as start_time, "
                          + "UTC_2_LOCAL_STR(HOP_END(proctime, INTERVAL '5' SECOND, INTERVAL '20' SECOND)) as end_time "
                     + "from KafkaTable GROUP BY custom_id, "
                                              + "HOP(proctime, INTERVAL '5' SECOND, INTERVAL '20' SECOND) "
                                      + "HAVING IS_DIFF(count(custom_id), custom_id)";
        Table resultTable = tableEnv.sqlQuery(sql);

        sinkToKafka(fromInput(pt, "output-topic"), kafkaProps, resultTable);
        env.execute("Job Name");
  }

    private static String fromInput(ParameterTool parameterTool, String key)
    {
        return parameterTool.getRequired(key);
    }

    private static Properties getKafkaPropsFromInput(final ParameterTool parameterTool)
    {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
        props.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));
        return props;
    }

  private static void sinkToKafka(String outputTopic, Properties props, Table resultTable)
  {
      Kafka010JsonTableSink tableSink = new Kafka010JsonTableSink(outputTopic, props, new FlinkFixedPartitioner<>());
      resultTable.writeToSink(tableSink);
  }
}
