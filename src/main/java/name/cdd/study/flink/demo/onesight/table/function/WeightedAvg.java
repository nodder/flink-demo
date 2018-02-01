package name.cdd.study.flink.demo.onesight.table.function;

import java.util.Iterator;

import org.apache.flink.table.functions.AggregateFunction;

//来自官网：https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/table/udfs.html
@SuppressWarnings ("serial")
public class WeightedAvg extends AggregateFunction<Long, WeightedAvg.WeightedAvgAccum>
{
    @Override
    public WeightedAvgAccum createAccumulator()
    {
        return new WeightedAvgAccum();
    }

    //Once all rows have been processed, the function is called to compute and return the final result.
    @Override
    public Long getValue(WeightedAvgAccum acc)
    {
        if(acc.count == 0)
        {
            return null;
        }
        else
        {
            return acc.sum / acc.count;
        }
    }

    //the function is called for each input row to update the accumulator
    public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight)
    {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }
    
    
    //Bellow functions are optional

    //required for aggregations on bounded OVER windows.
    public void retract(WeightedAvgAccum acc, long iValue, int iWeight)
    {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }

    //mandatory if the aggregation function should be applied in the context of a session group window 
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it)
    {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while(iter.hasNext())
        {
            WeightedAvgAccum a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    //required for many batch aggregations.
    public void resetAccumulator(WeightedAvgAccum acc)
    {
        acc.count = 0;
        acc.sum = 0L;
    }

    /**
     * Accumulator for WeightedAvg.
     */
    public static class WeightedAvgAccum
    {
        public long sum = 0;
        public int count = 0;
    }
}
