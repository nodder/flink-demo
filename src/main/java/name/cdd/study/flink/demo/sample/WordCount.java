package name.cdd.study.flink.demo.sample;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount
{
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,"
            );

        DataSet<Tuple2<String, Integer>> counts =
            text.flatMap(new LineSplitter())
            // group by the tuple field "0" and sum up tuple field "1"
            .groupBy(0)
            .sum(1);

        // execute and print result
        counts.print();
      }
    
    @SuppressWarnings ("serial")
    static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
          // normalize and split the line
          String[] tokens = value.toLowerCase().split("\\W+");

          // emit the pairs
          for (String token : tokens) {
            if (token.length() > 0) {
              out.collect(new Tuple2<String, Integer>(token, 1));
            }
          }
        }
      }
}
