package name.cdd.study.flink.demo.onesight.table.function;

import java.util.Hashtable;
import java.util.Optional;

public class CountDiffManager2
{
    private static CountDiffManager2 instance = new CountDiffManager2();
    
    private Hashtable<String, InnerData> key_to_innerData = new Hashtable<>();

    private CountDiffManager2()
    {
    }

    public static CountDiffManager2 getInstance()
    {
        return instance;
    }

    public boolean diff(String key, long num)
    {
        if(!key_to_innerData.containsKey(key))//TODO 多线程
        {
            return updateAndReturn(key, new InnerData(num), true);
        }
        
        InnerData innerData = key_to_innerData.get(key);
        boolean isDiff = innerData.num != num;
        return updateAndReturn(key, innerData.update(num), isDiff);
    }
    
    public Optional<Long> getVersion(String key)
    {//TODO 多线程
        return key_to_innerData.containsKey(key) ? Optional.of(key_to_innerData.get(key).version) : Optional.empty();
    }
    
    public void remove(String key)
    {
        key_to_innerData.remove(key);
    }

    private boolean updateAndReturn(String key, InnerData innerData, boolean isDiff)
    {
        key_to_innerData.put(key, innerData);
        return isDiff;
    }
    
    private class InnerData
    {
        private long num;
        private long version;
        
        public InnerData(long num)
        {
            this.num = num;
            this.version = 1;
        }
        
        public InnerData update(long newNum)
        {
            this.num = newNum;
            this.version++;
            return this;
        }
    }
}
