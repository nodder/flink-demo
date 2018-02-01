package name.cdd.study.flink.demo.onesight.connectors.kafka;

import java.util.Random;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

@SuppressWarnings ("serial")
public class FlinkRandomPartitioner<T> extends FlinkKafkaPartitioner<T> {

	private Random random = new java.util.Random();
	@Override
	public void open(int parallelInstanceId, int parallelInstances) {

	}

	@Override
	public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {

		Preconditions.checkArgument(partitions != null && partitions.length > 0,
				"Partitions of the target topic is empty.");

		int idx = random.nextInt(partitions.length);
		
		return partitions[idx];
	}

}
