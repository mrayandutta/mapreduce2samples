package org.mrunitsamples.groupby;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrunitsamples.util.MRUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Ayan on 10/20/2017.
 */
public class GroupByDriver
{
    public static class GroupMapper extends Mapper<LongWritable, Text, Country, IntWritable> {

        Country cntry = new Country();

        Text cntText = new Text();

        Text stateText = new Text();
        IntWritable populat = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] keyvalue = line.split(",");
            cntText.set(new Text(keyvalue[0]));
            stateText.set(keyvalue[1]);
            populat.set(Integer.parseInt(keyvalue[3]));
            Country cntry = new Country(cntText, stateText);
            context.write(cntry, populat);

            System.out.println("Running Mapper");

        }
    }

    public static class GroupReducer extends Reducer<Country, IntWritable, Country, IntWritable> {

        public void reduce(Country key, Iterator<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            int cnt = 0;
            while (values.hasNext()) {
                cnt = cnt + values.next().get();
            }
            System.out.println("Running Reducer");
            context.write(key, new IntWritable(cnt));

        }

    }

    /**
     *
     * The Country class implements WritabelComparator to implements custom sorting to perform group by operation. It
     * sorts country and then state.
     *
     */
    private static class Country implements WritableComparable<Country> {

        Text country;
        Text state;

        public Country(Text country, Text state) {
            this.country = country;
            this.state = state;
        }
        public Country() {
            this.country = new Text();
            this.state = new Text();

        }

        public void write(DataOutput out) throws IOException {
            this.country.write(out);
            this.state.write(out);

        }

        public void readFields(DataInput in) throws IOException {

            this.country.readFields(in);
            this.state.readFields(in);
            ;

        }

        public int compareTo(Country pop) {
            if (pop == null)
                return 0;
            int intcnt = country.compareTo(pop.country);
            if (intcnt != 0) {
                return intcnt;
            } else {
                return state.compareTo(pop.state);

            }
        }

        public String toString() {

            return country.toString() + ":" + state.toString();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = MRUtil.getLocalConfiguration(new Configuration());
        //Configuration conf = MRUtil.getRemoteConfiguration(new Configuration());

        Job job = Job.getInstance(conf, "GroupMR");
        job.setJarByClass(GroupByDriver.class);
        job.setMapperClass(GroupMapper.class);
        job.setCombinerClass(GroupReducer.class);
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Country.class);
        job.setOutputValueClass(IntWritable.class);
        //FileInputFormat.setMaxInputSplitSize(job, 10);
        //FileInputFormat.setMinInputSplitSize(job, 100);
        FileInputFormat.addInputPath(job, new Path(MRUtil.getValueByKey("groupby.input")));
        //MRUtil.deleteDir(MRUtil.getValueByKey("groupby.output"));
        FileOutputFormat.setOutputPath(job, new Path(MRUtil.getValueByKey("groupby.output")));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
