package name.cdd.study.flink.demo.onesight.connectors.kafka;

import java.util.Arrays;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.types.Row;

@SuppressWarnings ("serial")
public class OpenTsdbRowSerializationSchema implements SerializationSchema<Row> 
{
    private static ObjectMapper mapper = new ObjectMapper();
    private final String[] fieldNames;
    
    private String metricName;
    private int timestampIndex;
    private int valueIndex;
    
    public OpenTsdbRowSerializationSchema(RowTypeInfo rowSchema, String metricName, String timestampFieldName, String valueFieldName)
    {
        checkSchema(rowSchema);
        
        this.metricName = metricName;
        this.fieldNames = rowSchema.getFieldNames();
        this.timestampIndex = findTimeStampIndex(timestampFieldName);
        this.valueIndex = findValueIndex(valueFieldName);
    }

    private void checkSchema(RowTypeInfo rowSchema)
    {
        new JsonRowSerializationSchema(rowSchema);
    }

    @Override
    public byte[] serialize(Row row) {
        if (row.getArity() != fieldNames.length) {
            throw new IllegalStateException(String.format(
                "Number of elements in the row %s is different from number of field names: %d", row, fieldNames.length));
        }

        ObjectNode objectNode = mapper.createObjectNode();
        
        objectNode.put("metric", this.metricName);
        objectNode.put("timestamp", row.getField(this.timestampIndex).toString());
        objectNode.put("value", row.getField(this.valueIndex).toString());
        
        ObjectNode tagsNode = mapper.createObjectNode();
        objectNode.set("tags", tagsNode);
        
        for (int i = 0; i < row.getArity(); i++) {
            if(i != timestampIndex && i != valueIndex)
            {
                JsonNode node = mapper.valueToTree(row.getField(i));
                tagsNode.set(fieldNames[i], node);
            }
        }

        try {
            return mapper.writeValueAsBytes(objectNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row", e);
        }
    }
    
    private int findValueIndex(String valueFieldName)
    {
        for (int i = 0; i < fieldNames.length; i++) 
        {
            if(valueFieldName.equals(fieldNames[i]))
            {
                return i;
            }
        }
        
        throw new IllegalStateException(String.format("Cannot find value field %s in fields: %s", valueFieldName, Arrays.toString(fieldNames)));
    }

    private int findTimeStampIndex(String timestampFieldName)
    {
        for (int i = 0; i < fieldNames.length; i++) 
        {
            if(timestampFieldName.equals(fieldNames[i]))
            {
                return i;
            }
        }
        
        throw new IllegalStateException(String.format("Cannot find timestamp field %s in fields: %s", timestampFieldName, Arrays.toString(fieldNames)));
    }

}


