package name.cdd.study.flink.demo.onesight.table.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class PlusOne extends ScalarFunction {
    public static final String METHOD_NAME = "PLUS_ONE";

    private Map<Integer, Integer> day_to_num = new HashMap<>();
    
    public int eval(final int num, final int day) {
        if(!day_to_num.containsKey(day))
        {
            day_to_num.put(day, 0);
        }
        
        day_to_num.put(day, day_to_num.get(day) + num);
        System.out.println(day_to_num);
        return day_to_num.get(day);
    }
    
//    public int eval(int... num) {
//        if(!day_to_num.containsKey(num[1]))
//        {
//            day_to_num.put(num[1], 0);
//        }
//        
//        day_to_num.put(num[1], day_to_num.get(num[1]) + num[0]);
//        return day_to_num.get(num[1]);
//    }

}