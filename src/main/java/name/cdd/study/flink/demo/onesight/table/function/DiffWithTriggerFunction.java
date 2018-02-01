package name.cdd.study.flink.demo.onesight.table.function;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.runtime.tasks.AsyncExceptionHandler;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService;
import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class DiffWithTriggerFunction extends ScalarFunction implements AsyncExceptionHandler {
    public static final String METHOD_NAME = "IS_DIFF_WITH_EXPIRE_NOTIFY";
    
    private QueueConsumerSourceFunc expiringNotifyDsSource;
    private static ProcessingTimeService timerService;
    
    public DiffWithTriggerFunction(StreamExecutionEnvironment env, Properties kafkaProps, String outputTopic)
    {
        expiringNotifyDsSource = new QueueConsumerSourceFunc();
        DataStreamSource<String> expiringNotifyDs = env.addSource(expiringNotifyDsSource);
        expiringNotifyDs.addSink(new FlinkKafkaProducer011<>(
                        outputTopic,
                        new SimpleStringSchema(),
                        kafkaProps));
        
        timerService = new SystemProcessingTimeService(this, new Object(), threadFactory());
    }

    public boolean eval(String key, long num, long validityPeriod) {
      long expiringTime = System.currentTimeMillis() + validityPeriod;
      boolean result = CountDiffManager2.getInstance().diff(key, num);
      timerService.registerTimer(expiringTime, new ProcessTimeCallbak(key, CountDiffManager2.getInstance().getVersion(key), expiringNotifyDsSource));
      return result;
  }
    
    private ThreadFactory threadFactory()
    {
        return new DispatcherThreadFactory(new ThreadGroup("group_diff_function"), "Time Trigger for DiffWithExpiringFunction");
    }

    @Override
    public void handleAsyncException(String message, Throwable exception)
    {
        System.out.println("DiffWithExpiringFunction:" + message);
        exception.printStackTrace();
    }
    
    private class ProcessTimeCallbak implements ProcessingTimeCallback
    {
        private String key;
        private Optional<Long> lastVersion;
        private QueueConsumerSourceFunc additionalSource;

        public ProcessTimeCallbak(String key, Optional<Long> opVersion, QueueConsumerSourceFunc source)
        {
            this.key = key;
            this.lastVersion = opVersion;
            this.additionalSource = source;
        }
        
        @Override
        public void onProcessingTime(long timestamp) throws Exception
        {
            final Optional<Long> currVersion = CountDiffManager2.getInstance().getVersion(key);
            
            System.out.println("onProcessingTime | " + "key: " + key + "lastVersion: " + lastVersion + "; currVersion:" + currVersion + "; timestamp:" + timestamp);
            if(currVersion.isPresent() && isExpired(currVersion.get()))
            {
                System.out.println("onProcessingTime, expired info | key:" + key + "; timestamp:" + timestamp + ";currVersion:" + currVersion);
                CountDiffManager2.getInstance().remove(key);
                additionalSource.addMessage("Emit expiring notification |  key:" + key + ";timestamp:" + timestamp + ";version:" + currVersion);
            }
        }

        private boolean isExpired(long currVersion)
        {
            if(!lastVersion.isPresent())
            {
                return false;
            }
            return lastVersion.get() == currVersion;
        }
    }
    
    private static class QueueConsumerSourceFunc implements SourceFunction<String> {
        private static LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(true)
            {
                ctx.collect(messages.take());
            }
        }

        public void addMessage(String message)
        {
            messages.add(message);
            System.out.println("in add message, size " + messages.size());
        }

        @Override
        public void cancel() {
        }
    }
    
}