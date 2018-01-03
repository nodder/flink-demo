package name.cdd.study.flink.demo.onesight.connectors.kafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

public class KafkaOpenTsdbJsonTableSink extends Kafka010JsonTableSink
{
    private String opentsdbMetricName;
    private String opentsdbTimestampFieldName;
    private String opentsdbValueFieldName;

    public KafkaOpenTsdbJsonTableSink(String topic, Properties properties) {
        super(topic, properties, new FlinkFixedPartitioner<>());
    }

    public KafkaOpenTsdbJsonTableSink(String topic, Properties properties, FlinkKafkaPartitioner<Row> partitioner, String metricName, String timestampFieldName, String valueFieldName) {
        super(topic, properties, partitioner);
        this.opentsdbMetricName = metricName;
        this.opentsdbTimestampFieldName = timestampFieldName;
        this.opentsdbValueFieldName = valueFieldName;
    }
    
    @Override
    protected SerializationSchema<Row> createSerializationSchema(RowTypeInfo rowSchema) {
        return new OpenTsdbRowSerializationSchema(rowSchema, opentsdbMetricName, opentsdbTimestampFieldName, opentsdbValueFieldName);
    }
}
