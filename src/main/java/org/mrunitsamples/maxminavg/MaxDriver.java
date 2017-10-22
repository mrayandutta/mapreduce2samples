package org.mrunitsamples.maxminavg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrunitsamples.util.MRUtil;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Ayan on 10/20/2017.
 */
public class MaxDriver
{
    public static class MaxMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable offset, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer token = new StringTokenizer(value.toString());
            String employeeName=token.nextToken(",");
            int salary=Integer.parseInt(token.nextToken(","));

            Text newKey = new Text(employeeName);
            IntWritable newValue = new IntWritable(salary);
            context.write(newKey, newValue);
        }
    }

    public static class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            IntWritable maxTemperature = new IntWritable(Integer.MIN_VALUE);
            for (IntWritable value : values)
            {
                if (value.get() > maxTemperature.get())
                    maxTemperature = value;
            }

            context.write(key, maxTemperature);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = MRUtil.getLocalConfiguration(new Configuration());

        Job job = new Job(conf, MaxDriver.class.getName());
        job.setJarByClass(MaxDriver.class);

        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(MRUtil.getValueByKey("maxminavg.input")));
        FileOutputFormat.setOutputPath(job, new Path(MRUtil.getValueByKey("maxminavg.output")));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
