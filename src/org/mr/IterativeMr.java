package org.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IterativeMr
{
	public static class Mapper1 extends Mapper<Object, Text, Text, Text>
	{
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++)
			{
				try
				{
					Integer.parseInt(itr[i]);
				}
				catch(Exception e)
				{
					word.set(itr[i]);
					context.write(word, word);
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, NullWritable, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (Text val : values)
			{
				context.write(NullWritable.get(),val);
			}

		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++)
			{
				word.set(itr[i]);
				context.write(word, one);
			}
		}
	}

	public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/*",
				                "hdfs://master:9000/out1/",
				                "hdfs://master:9000/out1/*",
				                "hdfs://master:9000/out/" };
		Job job1 = new Job(conf, "word remove num"); // 设置一个用户定义的job名称
		job1.setJarByClass(IterativeMr.class);
		job1.setMapperClass(Mapper1.class); // 为job设置Mapper类
		job1.setReducerClass(Reducer1.class); // 为job设置Reducer类
		job1.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job1.setOutputValueClass(Text.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));// 为job设置输出路径
		job1.waitForCompletion(true); // 运行job

		Job job2 = new Job(conf, "word count"); // 设置一个用户定义的job名称
		job2.setJarByClass(IterativeMr.class);
		job2.setMapperClass(Mapper2.class); // 为job设置Mapper类
		job2.setReducerClass(Reducer2.class); // 为job设置Reducer类
		job2.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job2.setOutputValueClass(IntWritable.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));// 为job设置输出路径
		System.exit(job2.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
