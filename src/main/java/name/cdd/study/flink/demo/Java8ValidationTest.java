package name.cdd.study.flink.demo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

//https://ci.apache.org/projects/flink/flink-docs-release-1.0/apis/java8.html
public class Java8ValidationTest
{
    public static void main(String[] args) throws Exception
    {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3)
           .map(in -> new Tuple1<String>(" " + in))
           .print();
        
        //官网的下面这句话有问题。 print()方法自动会调用execute()方法，造成错误，所以注释掉env.execute()即可
//        env.execute();
    }
}
