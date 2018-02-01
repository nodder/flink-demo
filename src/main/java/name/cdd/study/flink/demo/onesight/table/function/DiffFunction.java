package name.cdd.study.flink.demo.onesight.table.function;

import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class DiffFunction extends ScalarFunction {
    public static final String METHOD_NAME = "IS_DIFF";
    
    public boolean eval(long num, String key) {
        
        return CountDiffManager.getInstance().diff(num, key);
    }
    
}