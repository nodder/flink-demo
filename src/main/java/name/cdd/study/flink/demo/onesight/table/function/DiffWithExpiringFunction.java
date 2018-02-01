package name.cdd.study.flink.demo.onesight.table.function;

import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class DiffWithExpiringFunction extends ScalarFunction{
    public static final String METHOD_NAME = "IS_DIFF_WITH_EXPIRING";
    
    public boolean eval(long num, String key, long currTime, long validityPeriod) {
        return CountDiffWithExpiringManager.getInstance().diff(num, key, currTime, validityPeriod);
    }
    
}