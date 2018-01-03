package name.cdd.study.flink.demo.onesight.table.function;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class UTCToLocalStr extends ScalarFunction {
    public static final String METHOD_NAME = "UTC_2_LOCAL_STR";

    public String eval(final long t) {
        final Timestamp estTime = new Timestamp(t);
        return estTime.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}