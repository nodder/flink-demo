package name.cdd.study.flink.demo.onesight.table.function;

import java.sql.Timestamp;

import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings ("serial")
public class UTCToLocal extends ScalarFunction {
    public static final String METHOD_NAME = "UTC_2_LOCAL";

    public Timestamp eval(final long t) {

        final Timestamp estTime = new Timestamp(t);

        return estTime;
    }

}