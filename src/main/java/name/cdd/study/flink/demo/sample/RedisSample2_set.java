package name.cdd.study.flink.demo.sample;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

//参考：https://github.com/apache/bahir-flink/blob/master/flink-connector-redis/src/test/java/org/apache/flink/streaming/connectors/redis/RedisSinkITCase.java
public class RedisSample2_set
{
    private static final int NUM_ELEMENTS = 20;
    
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(6379).build();
        
        DataStreamSource<String> source = env.addSource(new TestSourceFunction());

        RedisSink<String> redisSink = new RedisSink<>(jedisPoolConfig, new RedisSetMapper());

        source.addSink(redisSink);

        env.execute("Redis Sink Test");
    }
    
    @SuppressWarnings ("serial")
    private static class TestSourceFunction implements SourceFunction<String> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < NUM_ELEMENTS && running; i++) {
                ctx.collect(String.format("{\"trans_time\":\"1514429670429\", \"amount\":%d, \"host\":\"192.168.2.1\",\"trans_type\":\"2\",\"is_success\":true, \"card_ass\":\"DOMN\", \"merchant\":\"Tenpay\"}", 
                            i*1000));
                
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
    
    @SuppressWarnings ("serial")
    static class RedisSetMapper implements RedisMapper<String> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(String data) {
            return "cdd";
        }

        @Override
        public String getValueFromData(String data) {
            return data;
        }
    }
}
