package name.cdd.study.flink.demo.sample;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

//参考：https://github.com/apache/bahir-flink/blob/master/flink-connector-redis/src/test/java/org/apache/flink/streaming/connectors/redis/RedisSinkPublishITCase.java
public class RedisSample1_subscribe
{
    private static final int NUM_ELEMENTS = 20;
    private static final String REDIS_CHANNEL = "CHANNEL";
    
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                        .setHost("127.0.0.1")
                        .setPort(6379).build();
        
        DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunction());

        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig, new RedisTestMapper());

        source.addSink(redisSink);

        env.execute("Redis Sink Test");
    }
    
    @SuppressWarnings ("serial")
    private static class TestSourceFunction implements SourceFunction<Tuple2<String, String>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            for (int i = 0; i < NUM_ELEMENTS && running; i++) {
                ctx.collect(new Tuple2<>(REDIS_CHANNEL, "message #" + i));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
    
    @SuppressWarnings ("serial")
    private static class RedisTestMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.PUBLISH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
