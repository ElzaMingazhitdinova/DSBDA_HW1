import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    private final String testDataReducer = "2020-01-04 12";
    private final String testDataMapper = "2020-01-04 12:01:01,4";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        HashMap <String, Integer> statistic = new HashMap<>();
        statistic.put("warning", 1);
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testDataMapper))
                .withOutput(new Text(testDataReducer), new Text(statistic.toString()))
                .runTest();
    }

    @Test
    public void testDataMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testDataMapper))
                .withOutput(new Text(testDataReducer), new IntWritable(4))
                .runTest();
    }

}