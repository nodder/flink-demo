package name.cdd.study.flink.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterativeStreamDemo
{
    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Long> someIntegers = env.generateSequence(5, 5);//(5, 6)

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
          @Override
          public Long map(Long value) throws Exception {
              System.out.println("before minus, value=" + value);
              return value - 1 ;
          }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
          @Override
          public boolean filter(Long value) throws Exception {
              System.out.println("in filter 1, value=" + value);
              return (value > 0);
          }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
          @Override
          public boolean filter(Long value) throws Exception {
              System.out.println("in filter 2, value=" + value);
              return (value <= 0);
          }
        });
        
        lessThanZero.print();
        env.execute();
    }

}
