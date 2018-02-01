package name.cdd.study.flink.demo.onesight.table.function;

import java.util.Hashtable;

public class CountDiffManager
{
    private static CountDiffManager instance = new CountDiffManager();
    
    private Hashtable<String, Long> key_to_count = new Hashtable<>();

    private CountDiffManager()
    {
    }

    public static CountDiffManager getInstance()
    {
        return instance;
    }

    public boolean diff(long num, String key)
    {
        System.out.println("key" + key);
        if(!key_to_count.containsKey(key))
        {
            return updateAndReturn(num, key, true);
        }
        
        boolean isDiff = key_to_count.get(key) != num;
        return updateAndReturn(num, key, isDiff);
    }

    private boolean updateAndReturn(long num, String key, boolean isDiff)
    {
        key_to_count.put(key, num);
        return isDiff;
    }
}
