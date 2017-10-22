package org.mrunitsamples.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrunitsamples.util.MRUtil;

import java.io.IOException;

/**
 * Created by Ayan on 10/22/
 * Problem :
 * 1. Given a list of employees with there department and salary find the maximum and minimum salary in each department.
 * 2. Given a list of employees with there department find the count of employees in each department.
 */
public class EmployeeMinMaxCountDriver
{
    public static class EmployeeMinMaxCountMapper extends Mapper<Object, Text, Text, CustomMinMaxTuple> {

        private CustomMinMaxTuple outTuple = new CustomMinMaxTuple();
        private Text departmentName = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] field = data.split(",", -1);
            double salary = 0;

            if (null != field && field.length == 9 && field[7].length() >0) {

                salary=Double.parseDouble(field[7]);
                outTuple.setMin(salary);
                outTuple.setMax(salary);
                outTuple.setCount(1);
                departmentName.set(field[3]);
                //As we need to group by dept,let it be the key to reducer
                context.write(departmentName, outTuple);
            }
        }
    }

    public static class EmployeeMinMaxCountReducer extends Reducer<Text, CustomMinMaxTuple, Text, CustomMinMaxTuple>
    {
        private CustomMinMaxTuple result = new CustomMinMaxTuple();
        public void reduce(Text key, Iterable<CustomMinMaxTuple> values, Context context)
                throws IOException, InterruptedException
        {
            result.setMin(null);
            result.setMax(null);
            result.setCount(0);
            long sum = 0;
            for (CustomMinMaxTuple minMaxCountTuple : values)
            {
                if (result.getMax() == null || (minMaxCountTuple.getMax() > result.getMax()))
                {
                    result.setMax(minMaxCountTuple.getMax());
                }
                if (result.getMin() == null || (minMaxCountTuple.getMin() < result.getMin()))
                {
                    result.setMin(minMaxCountTuple.getMin());
                }
                sum = sum + minMaxCountTuple.getCount();
                result.setCount(sum);
            }
            context.write(new Text(key.toString()), result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = MRUtil.getLocalConfiguration(new Configuration());
        //Configuration conf = MRUtil.getRemoteConfiguration(new Configuration());

        Job job = Job.getInstance(conf, "MaxMinCount");
        job.setJarByClass(EmployeeMinMaxCountDriver.class);
        job.setJobName("MaxMinCount");
        FileInputFormat.addInputPath(job, new Path(MRUtil.getValueByKey("aggregation.input")));
        FileOutputFormat.setOutputPath(job, new Path(MRUtil.getValueByKey("aggregation.output")));

        job.setMapperClass(EmployeeMinMaxCountMapper.class);
        job.setReducerClass(EmployeeMinMaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomMinMaxTuple.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
