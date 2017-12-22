package name.cdd.study.flink.demo;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

//https://segmentfault.com/a/1190000005595920
//http://flink.apache.org/news/2016/05/24/stream-sql.html
//-input-topic fin -output-topic fout -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -group-id flink-group
//group-id可选

//{"id": 1001, "name":"cdd", "score":100}
//{"id": 1001, "name":"baly", "score":50}
public class TableDemo_sql_kafka
{
    public static void main(String[] args) throws Exception
    {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputTopic = parameterTool.getRequired("input-topic");
        String outputTopic = parameterTool.getRequired("output-topic");
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
        props.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));

//        TypeInformation<Row> typeInfo = Types.ROW(
//            new String[] { "id", "name", "score" },
//            new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.INT() }
//          );
        TableSchema ts = new TableSchema(new String[] { "id", "name", "score" }, new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.INT()});
        
        Kafka011JsonTableSource tableSource = new Kafka011JsonTableSource(inputTopic, props, ts, ts);
        // Fail on missing JSON field
        //By default, a missing JSON field does not fail the source
        tableSource.setFailOnMissingField(false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        tableEnv.registerTableSource("table1", tableSource);
        
        /** 选择一种情况放开验证 start */
//        case1_append_only_to_csv(tableEnv);
//        case2_append_only_to_kafka(outputTopic, props, tableEnv);
        case3_append_only_print(tableEnv);
//        case4_retract_print(tableEnv);
        
        /** 选择一种情况放开验证 end */
                
        env.execute(     );
    }
    
    //输入{"id": 1001, "name":"cdd", "score":110}，输出：2> 1001,cdd,110
    static void case3_append_only_print(StreamTableEnvironment tableEnv)
    {
        Table resultTable = tableEnv.sql("select * from table1 where score>=60");//这个可以使用toAppendStream或者toRetractStream
        DataStream<Row> resultDs = tableEnv.toAppendStream(resultTable, Row.class);
        resultDs.print();
    }
    
    //输入{"id": 1001, "name":"cdd", "score":110}，输出3> (true,1001,110)
    //再输入{"id": 1001, "name":"aaa", "score":120}，输出3> (false,1001,110) \n3> (true,1001,230)
    static void case4_retract_print(StreamTableEnvironment tableEnv)
    {
        Table resultTable = tableEnv.sql("select id, sum(score) from table1 where score>=60 group by id"); //这个只能使用toRetractStream
        DataStream<Tuple2<Boolean, Row>> resultDs = tableEnv.toRetractStream(resultTable, Row.class);
        resultDs.print();
    }

    //输入{"id": 1001, "name":"cdd", "score":110}，输出：{"id":1001,"name":"cdd","score":110}
    static void case2_append_only_to_kafka(String outputTopic, Properties props, StreamTableEnvironment tableEnv)
    {
        Table resultTable = tableEnv.sql("select * from table1 where score>=60");//这个可以使用toAppendStream或者toRetractStream
        Kafka09JsonTableSink tableSink = new Kafka09JsonTableSink(outputTopic, props, new FlinkFixedPartitioner<Row>());//Kafka09JsonTableSink只能用于append-only的table
        resultTable.writeToSink(tableSink);
    }

    //输入{"id": 1001, "name":"cdd", "score":110}，输出：1001|cdd|110
    static void case1_append_only_to_csv(StreamTableEnvironment tableEnv)
    {
        Table resultTable = tableEnv.sql("select * from table1 where score>=60");//这个可以使用toAppendStream或者toRetractStream        
        //CsvTableSink第三个参数是生成文件数，本机测试默认是4。如果是1，则table_output是文件名，如果大于1，则table_output是目录名，该目录下生成1, 2, 3...这样的文件。
        //CsvTableSink支持batch和append-only的table
        resultTable.writeToSink(new CsvTableSink("src/main/resources/table_output", "|", 1, WriteMode.OVERWRITE));
    }
}
