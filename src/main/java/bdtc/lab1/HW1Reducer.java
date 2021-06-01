package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Reducer class
 * Input key {@link Text}
 * Input value {@link IntWritable}
 * Output key {@link Text}
 * Output value {@link Text}
 */
public class HW1Reducer extends Reducer<Text, IntWritable,Text, Text> {

    /**
     * List of errors
     */
    private static final String[] errorType = {" panic", "alert", "crit", "error",
            "warning", "notice", "info", "debug"};


    /**
     * Reduce function. Counts the number of errors by types.
     *
     * @param key     Text
     * @param values  iterable of values
     * @param context reducer context
     * @throws IOException          in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        HashMap<String, Integer> statistic = new HashMap<>();
        while (values.iterator().hasNext()) {
            int errorCode = values.iterator().next().get();
            String extendedKey = errorType[errorCode];

            if (statistic.containsKey(extendedKey)) {
                statistic.put(extendedKey, statistic.get(extendedKey) + 1);
            } else {
                statistic.put(extendedKey, 1);
            }
        }
        context.write(key, new Text(statistic.toString()));
    }
}
