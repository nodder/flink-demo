package name.cdd.study.flink.demo.onesight.table.function;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.table.functions.AggregateFunction;

@SuppressWarnings ("serial")
public class SumWithinADay extends AggregateFunction<Map<Integer, Integer>, SumWithinADay.SumOfDay> {
    public static final String METHOD_NAME = "Sum_OneDay";
    
    //对于每个group，都会创建各自的accumulator
    @Override
    public SumOfDay createAccumulator()
    {
        return new SumOfDay();
    }

    //Once all rows have been processed, the function is called to compute and return the final result.
    @Override
    public Map<Integer, Integer> getValue(SumOfDay acc)
    {
        System.out.println("getValue SumWithinADay:" + acc.day_to_sum);
        
        //这里clone，是因为如果getValue的值没有变化，则聚合函数不会有输出。
        //只有getValue的值变化了，才会在toRectractStream方法输出一个false和true的结果。
        //而对于pojo对象，是通过引用地址的变化（而不是对象内容变化）来判断getValue的值是否变化
        //从代码本身而言，可以不使用Map作为返回值（这里Map永远只有一个值，因为sql语句限定是group by theDay），而且更简单。这里只是对此机制进行demo说明。
        Map<Integer, Integer> clone = Maps.newHashMap();
        clone.putAll(acc.day_to_sum);
        return clone;
    }

    //the function is called for each input row to update the accumulator
    public void accumulate(SumOfDay acc, int day, int sum)
    {
        System.out.println("before accumulate:" + acc.day_to_sum + ";count=" +acc.count);
        if(!acc.day_to_sum.containsKey(day))
        {
            acc.day_to_sum.put(day, 0);
        }
        
        acc.day_to_sum.put(day, sum + acc.day_to_sum.get(day));
        acc.count++;
        
        System.out.println("after accumulate:" + acc.day_to_sum + ";count=" +acc.count);
        
    }
    
    //以下可选，不需要写
    //required for aggregations on bounded OVER windows.
    public void retract(SumOfDay acc, int day, int sum)
    {
        System.out.println("in retract");
        if(!acc.day_to_sum.containsKey(day))
        {
            acc.day_to_sum.put(day, 0);
        }
        
        acc.day_to_sum.put(day, sum - acc.day_to_sum.get(day));
    }

    //mandatory if the aggregation function should be applied in the context of a session group window 
    public void merge(SumOfDay acc, Iterable<SumOfDay> it)
    {
        System.out.println("in merge");
        Iterator<SumOfDay> iter = it.iterator();
        while(iter.hasNext())
        {
            SumOfDay a = iter.next();
            
            Set<Integer> keySet = a.day_to_sum.keySet();
            
            for(Integer key : keySet)
            {
                if(!acc.day_to_sum.containsKey(key))
                {
                    acc.day_to_sum.put(key, 0);
                }
                
                acc.day_to_sum.put(key, a.day_to_sum.get(key) + acc.day_to_sum.get(key));
            }
        }
    }

    //required for many batch aggregations.
    public void resetAccumulator(SumOfDay acc)
    {
        System.out.println("in reset");
        acc.day_to_sum.clear();
    }
    

    public static class SumOfDay
    {
        private Map<Integer, Integer> day_to_sum = new HashMap<>();
        private int count = 0;
        
        public SumOfDay()
        {
            System.out.println("init SumOfDay");
        }
    }
    
}