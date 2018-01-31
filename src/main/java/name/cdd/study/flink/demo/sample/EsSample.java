package name.cdd.study.flink.demo.sample;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

//-input-topic fin3 -bootstrap-server hadoop2:9092,hadoop3:9092,hadoop4:9092 -es-cluster-name my-es-cluster -es-bulk-flush-max-actions 500 -es-bulk-flush-interval-ms 1000
public class EsSample
{
    @SuppressWarnings ("serial")
    public static void main(String[] args) throws Exception
    {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputTopic = parameterTool.getRequired("input-topic");
        
        Properties inputProps = new Properties();
        inputProps.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap-server"));
        inputProps.setProperty("group.id", parameterTool.get("group-id", "myGroupId"));
        
        Map<String, String> outputMap = new HashMap<>();
        outputMap.put("cluster.name", parameterTool.getRequired("es-cluster-name"));
        // This instructs the sink to emit after every element, otherwise they would be buffered
        outputMap.put("bulk.flush.max.actions", parameterTool.getRequired("es-bulk-flush-max-actions"));
        // Interval at which to flush regardless of the amount or size of buffered actions.
        outputMap.put("bulk.flush.interval.ms", parameterTool.getRequired("es-bulk-flush-interval-ms"));
        
        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("hadoop2"), 9300));//用IP和主机名都可以。spark的程序中用的9200， flink用的9300
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("hadoop3"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("hadoop4"), 9300));
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(inputTopic,new SimpleStringSchema(),inputProps);
        consumer.setStartFromLatest();
        DataStream<String> input = env.addSource(consumer);
        
//        input.print();
        
        input.addSink(new ElasticsearchSink<>(outputMap, transportAddresses, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
            
            private IndexRequest createIndexRequest(String element) {
                return Requests.indexRequest().
                        index("myflink2")
                        .type("mytype")
                        .source(element);
            }
        }));
        
        env.execute("Kafka to ES");
    }


}
