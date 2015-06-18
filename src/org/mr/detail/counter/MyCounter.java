package org.mr.detail.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyCounter {

	public static class MyCounterMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public static Counter ct = null;

		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			String arr_value[] = value.toString().split(" ");
			if (arr_value.length > 3) {
				ct = context.getCounter("ErrorCounter", "toolong");
				ct.increment(1);
			} else if (arr_value.length < 3) {
				ct = context.getCounter("ErrorCounter", "tooshort");
				ct.increment(1);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/access.log",
				"hdfs://master:9000/uvout" };

		Job job = new Job(conf, "MyCounter");
		job.setJarByClass(MyCounter.class);
		job.setMapperClass(MyCounterMap.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}