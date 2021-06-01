package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Mapper class
 * Input key {@link LongWritable}
 * Input value {@link Text}
 * Output key {@link Text}
 * Output value {@link IntWritable}
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Regex pattern to check if the input row is correct
     */
    private final static Pattern regular = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},[0-7]$");
    private final static IntWritable one = new IntWritable(1);

    /**
     * Regex pattern to split input row
     */
    private final static Pattern splitter = Pattern.compile(":|,");

    /**
     * Map function. Checks data for correctness and determines the hours and types of errors
     * Uses counters {@link CounterType}
     *
     * @param key     input key
     * @param value   input value
     * @param context mapper context
     * @throws IOException          from context.write()
     * @throws InterruptedException from context.write()
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (regular.matcher(line).matches()) {
            String[] parts = splitter.split(line);
            one.set(Integer.parseInt(parts[3]));
            context.write(new Text(parts[0]), one);
        } else {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}