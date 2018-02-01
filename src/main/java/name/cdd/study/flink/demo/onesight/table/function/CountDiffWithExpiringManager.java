package name.cdd.study.flink.demo.onesight.table.function;

import java.util.Hashtable;

public class CountDiffWithExpiringManager
{
    private static CountDiffWithExpiringManager instance = new CountDiffWithExpiringManager();
    
    private Hashtable<String, ExpiringNum> key_to_count = new Hashtable<>();

    private CountDiffWithExpiringManager()
    {
    }

    public static CountDiffWithExpiringManager getInstance()
    {
        return instance;
    }

    public boolean diff(long num, String key, long currTime, long validityPeriod)
    {
        boolean isDiff = !key_to_count.containsKey(key) || isExpired(key, currTime) || key_to_count.get(key).num != num;
        
        System.out.println("num:" + num + ";key:" + key + ";currTime:" + currTime + ";validityPeriod:" + validityPeriod + ";isDiff:" + isDiff);
        key_to_count.put(key, new ExpiringNum(num, currTime));
        return isDiff;    
    }
    
    private boolean isExpired(String key, long currTime)
    {
        System.out.println("in isExpired:" + key_to_count.get(key).expiringTime + "; currTime:" + currTime + "| result:" + (key_to_count.get(key).expiringTime + 60000 < currTime));
        return key_to_count.get(key).expiringTime + 60000 < currTime;
    }

    class ExpiringNum
    {
        long num;
        long expiringTime;
        
        ExpiringNum(long num, long expiringTime)
        {
            this.num = num;
            this.expiringTime = expiringTime;
        }
    }
}
