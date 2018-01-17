package name.cdd.study.flink.demo.onesight.connectors.redis;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;

@SuppressWarnings ("serial")
public class RedisSetRowMapper implements RedisMapper<Row>
{
    private static ObjectMapper mapper = new ObjectMapper();
    
    private String[] columnNames;
    private String key;
    
    public RedisSetRowMapper(String key, String[] columnNames)
    {
        this.columnNames = columnNames;
        this.key = key;
    }
    
    public RedisSetRowMapper(String key)
    {
        this.key = key;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(Row row) {
        return key;
    }

    @Override
    public String getValueFromData(Row row) {
        if(this.columnNames == null)
        {
            return row.toString();
        }
        
        ObjectNode objectNode = mapper.createObjectNode();

        for (int i = 0; i < row.getArity(); i++) {
            JsonNode node = mapper.valueToTree(row.getField(i));
            objectNode.set(columnNames[i], node);
        }
        
        return objectNode.toString();
    }
}
