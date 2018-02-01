package name.cdd.study.flink.demo.onesight.connectors.redis;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;

//https://ci.apache.org/projects/flink/flink-docs-release-1.1/api/java/
// LPUSH LPUSH languages python  LPUSH languages python  LRANGE languages 0 -1 => "python" "python" （允许重复）
// RPUSH 
//SADD  将一个或多个member 元素加入到集合key | sadd bookset b1
//ZADD 将一个或多个member 元素及其score 值加入到有序集key | zadd zbookset 1 b1  | zrange zbookset 0 -1 withscores
//HSET | HSET key field value | hset bset b1 abcde
//PFADD 
@SuppressWarnings ("serial")
public class RedisHsetRowMapper implements RedisMapper<Row> {
    
    private String[] columnNames;
    
    private String redisKey;
    private String hsetField;
    private String hsetFiledValue;
    
    public RedisHsetRowMapper(String redisKey, String[] columnNames, String hsetField, String hsetFiledValue)
    {
        this.columnNames = columnNames;
        this.redisKey = redisKey;
        this.hsetField = hsetField;
        this.hsetFiledValue = hsetFiledValue;
    }
    
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, redisKey);
    }

    @Override
    public String getKeyFromData(Row data) {
        int index = findIndex(columnNames, hsetField);
        System.out.println("getKeyFromData " + data.getField(index).toString());
        return data.getField(index).toString();
    }

    //TODO 支持将整个json作为value，而不是某个字段
    @Override
    public String getValueFromData(Row data) {
        int index = findIndex(columnNames, hsetFiledValue);
        System.out.println("getValueFromData " + data.getField(index).toString());
        return data.getField(index).toString();
    }
    
    private int findIndex(String[] columnNames, String field)
    {
        for(int i = 0; i < columnNames.length; i++)
        {
            if(columnNames[i].equals(field))
            {
                return i;
            }
        }
        
        return -1;
    }
}
