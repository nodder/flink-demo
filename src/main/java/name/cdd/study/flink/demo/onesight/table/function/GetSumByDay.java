package name.cdd.study.flink.demo.onesight.table.function;

import java.util.Map;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class GetSumByDay extends ScalarFunction {
    public static final String METHOD_NAME = "GET_SUM";
    private Map<Integer, Integer> day_to_sum = Maps.newHashMap();//總的；而每個agg方法，都是每個group的
    
    public int eval(Map<Integer, Integer> acc, int theDay) {
        for(int key : acc.keySet())//always1
        {
            day_to_sum.put(key, acc.get(key));
        }
            
        return day_to_sum.get(theDay);
    }
    
}