package name.cdd.study.flink.demo.onesight.connectors.redis;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;

@SuppressWarnings ("serial")
public class RedisPublishRowMapper implements RedisMapper<Row>
{
    private static ObjectMapper mapper = new ObjectMapper();
    
    private String[] columnNames;
    private String channel;
    
    public RedisPublishRowMapper(String channel, String[] columnNames)
    {
        this.columnNames = columnNames;
        this.channel = channel;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.PUBLISH);
    }

    @Override
    public String getKeyFromData(Row row) {
        return channel;
    }

    @Override
    public String getValueFromData(Row row) {
        ObjectNode objectNode = mapper.createObjectNode();

        for (int i = 0; i < row.getArity(); i++) {
            JsonNode node = mapper.valueToTree(row.getField(i));
            objectNode.set(columnNames[i], node);
        }
        
        return objectNode.toString();
    }
}
