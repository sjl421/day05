package org.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.util.AnalysisNginxTool;
import org.util.DateToNUM;

public class AccessLogUvMr {
	public static class AccessLogUvMrMapper extends
			Mapper<Object, Text, Text, Text> {
		private final static Text ip = new Text("");
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split(" ");
			if (itr.length < 7) {
				return;
			}
			String date = AnalysisNginxTool.nginxDateStmpToDate(itr[3]);
			String url=itr[6];
			word.set(date+"_"+url);
			ip.set(itr[0]);
			context.write(word, ip);
		}
	}

	public static class AccessLogUvMrReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Set<String> ipset=new HashSet<String>();
			for (Text val : values) {
				ipset.add(val.toString());
			}
			result.set(ipset.size());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		DateToNUM.initMap();
		Configuration conf = new Configuration();
		String[] otherArgs = {"hdfs://master:9000/access.log","hdfs://master:9000/uvout"};
		Job job = new Job(conf, "uv"); // 设置一个用户定义的job名称
		job.setJarByClass(AccessLogUvMr.class);
		job.setMapperClass(AccessLogUvMrMapper.class); // 为job设置Mapper类
		job.setReducerClass(AccessLogUvMrReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
