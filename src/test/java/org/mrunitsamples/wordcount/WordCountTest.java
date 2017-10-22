package org.mrunitsamples.wordcount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mrunitsamples.wordcount.WordCount.IntSumReducer;
import org.mrunitsamples.wordcount.WordCount.TokenizerMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class WordCountTest
{
    private Mapper<Object, Text, Text, IntWritable> mapper;
    private Reducer<Text, IntWritable, Text, IntWritable> reducer;

    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp(){
        mapper = new TokenizerMapper();
        reducer = new IntSumReducer();

        mapDriver = new MapDriver<Object, Text, Text, IntWritable>(mapper);
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
        mapReduceDriver = new MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException{
        mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapDriver.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver.withOutput(new Text("dog"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("cat"), values);
        reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException{
        mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
