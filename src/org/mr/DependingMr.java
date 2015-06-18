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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DependingMr {
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++) {
				try {
					Integer.parseInt(itr[i]);
				} catch (Exception e) {
					word.set(itr[i]);
					context.write(word, word);
				}
			}
		}
	}

	public static class Reducer1 extends
			Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(NullWritable.get(), val);
			}

		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++) {
				try {
					Integer.parseInt(itr[i]);
				} catch (Exception e) {
					word.set(itr[i]);
					context.write(word, word);
				}
			}
		}
	}

	public static class Reducer2 extends
			Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(NullWritable.get(), val);
			}

		}
	}

	public static class Mapper3 extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split("");
			for (int i = 0; i < itr.length; i++) {
				word.set(itr[i]);
				context.write(word, one);
			}
		}
	}

	public static class Reducer3 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/test1/a.txt",
				"hdfs://master:9000/test1/b.txt", "hdfs://master:9000/out1/",
				"hdfs://master:9000/out2/", "hdfs://master:9000/out1/*",
				"hdfs://master:9000/out2/*", "hdfs://master:9000/out/" };
		Job job1 = new Job(conf, "word remove num1"); // 设置一个用户定义的job名称
		job1.setJarByClass(IterativeMr.class);
		job1.setMapperClass(Mapper1.class); // 为job设置Mapper类
		job1.setReducerClass(Reducer1.class); // 为job设置Reducer类
		job1.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job1.setOutputValueClass(Text.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));// 为job设置输出路径

		Job job2 = new Job(conf, "word remove num12"); // 设置一个用户定义的job名称
		job2.setJarByClass(IterativeMr.class);
		job2.setMapperClass(Mapper2.class); // 为job设置Mapper类
		job2.setReducerClass(Reducer2.class); // 为job设置Reducer类
		job2.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job2.setOutputValueClass(Text.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));// 为job设置输出路径
		
		Job job3 = new Job(conf, "word count"); // 设置一个用户定义的job名称
		job3.setJarByClass(IterativeMr.class);
		job3.setMapperClass(Mapper3.class); // 为job设置Mapper类
		job3.setReducerClass(Reducer3.class); // 为job设置Reducer类
		job3.setOutputKeyClass(Text.class); // 为Map的输出数据设置Key类
		job3.setOutputValueClass(IntWritable.class); // 为Map输出设置value类
		FileInputFormat.addInputPath(job3, new Path(otherArgs[4])); // 为job设置输入路径
		FileInputFormat.addInputPath(job3, new Path(otherArgs[5])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[6]));// 为job设置输出路径

		ControlledJob controlledJob1 = new ControlledJob(
				job1.getConfiguration());
		controlledJob1.setJob(job1);
		
		ControlledJob controlledJob2 = new ControlledJob(
				job2.getConfiguration());
		controlledJob2.setJob(job2);
		
		ControlledJob controlledJob3 = new ControlledJob(
				job3.getConfiguration());
		controlledJob3.setJob(job3);
		
		controlledJob3.addDependingJob(controlledJob1);
		controlledJob3.addDependingJob(controlledJob2);
		JobControl jc = new JobControl("test");
		jc.addJob(controlledJob1);
		jc.addJob(controlledJob2);
		jc.addJob(controlledJob3);
		Thread jcThread = new Thread(jc);//如果直接调用run，线程无法结束
		jcThread.start();
		while (true) {
			if (jc.allFinished()) {
				System.out.println(jc.getSuccessfulJobList());
				jc.stop();
				System.exit(0);
			}
			if (jc.getFailedJobList().size() > 0) {
				System.out.println(jc.getFailedJobList());
				jc.stop();
				System.exit(1);
			}
		}
	}
}
