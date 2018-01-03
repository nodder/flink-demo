package name.cdd.study.flink.demo.sample;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import name.cdd.study.flink.demo.onesight.table.function.PlusOne;

//https://segmentfault.com/a/1190000005595920
//http://flink.apache.org/news/2016/05/24/stream-sql.html
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//group-id可选

//{"id": 1001, "name":"cdd", "score":100}
//{"id": 1001, "name":"baly", "score":50}
public class TableDemo_tableFunction
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
        
        tableEnv.registerFunction(PlusOne.METHOD_NAME, new PlusOne());
        
        KafkaTableSource tableSource = Kafka011JsonTableSource.builder()
                        .forTopic(inputTopic)
                        .withKafkaProperties(props)
                        .withSchema(TableSchema.builder()
                                .field("id", Types.STRING())
                                .field("theDay", Types.INT())
                                .field("score", Types.INT()).build())
                        .failOnMissingField(false)
                        .build();
        
        tableEnv.registerTableSource("table1", tableSource);
        
//        {"id": 1001, "score":10, "theDay":1}
//        {"id": 1001, "score":11, "theDay":1}
//        {"id": 1001, "score":100, "theDay":2}
//        {"id": 1001, "score":101, "theDay":2}
//        {"id": 1001, "score":22, "theDay":1}
        Table resultTable = tableEnv.sqlQuery("select id, PLUS_ONE(score, theDay) from table1 group by id"); 
        DataStream<Row> resultDs = tableEnv.toDataStream(resultTable, Row.class);
        resultDs.print();
        
        env.execute("TableDemo_sql_kafka");
    }
}
