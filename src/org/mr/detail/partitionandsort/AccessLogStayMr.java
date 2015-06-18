package org.mr.detail.partitionandsort;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.util.AnalysisNginxTool;
import org.util.DateToNUM;

public class AccessLogStayMr {
	public static class AccessLogStayMrMapper extends
			Mapper<Object, Text, Text, Text> {
		private final static Text k = new Text("");
		private Text v = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split(" ");
			if (itr.length < 7) {
				return;
			}
			long date = AnalysisNginxTool.nginxDateStmpToDateTime(itr[3]);
			String url = itr[6];
			k.set(itr[0] + "|" + date);
			v.set(url);
			context.write(k, v);

		}
	}

	public static class FirstPartitioner extends
			Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value,
				int numPartitions) {
			String k=key.toString().split("\\|")[0];
			return k.hashCode() % numPartitions;
		}
	}
	
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(Text.class, true);
		}

		@Override
		// Compare two WritableComparables.
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text a = (Text) w1;
			Text b = (Text) w2;
			int l = a.toString().split("\\|")[0].hashCode();
			int r = b.toString().split("\\|")[0].hashCode();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	public static class AccessLogStayMrReducer extends
			Reducer<Text, Text, Text, NullWritable> {
		private Text k = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String urltmp = "";
			for (Text val : values) {
				System.out.println(urltmp);
				k.set(urltmp + "|=end=|" + key.toString() + "|"
						+ val.toString());
				if (!urltmp.equals("")) {
					context.write(k, NullWritable.get());
				}
				urltmp = key.toString() + "|" + val.toString();
			}
		}
	}

	public static class AccessLogStayMrMapper2 extends
			Mapper<Object, Text, Text, LongWritable> {
		private final static Text k = new Text("");
		private LongWritable v = new LongWritable(0);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split("\\|=end=\\|");
			if (itr.length != 2) {
				return;
			}
			String str1[] = itr[0].split("\\|");
			String str2[] = itr[1].split("\\|");

			String ip = str1[0];
			long datetime1 = Long.parseLong(str1[1]);
			String url = str1[2];
			long datetime2 = Long.parseLong(str2[1]);
			Date d = new Date(datetime1);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
			System.out.println(sdf1.format(d));
			k.set(ip + "|" + url + "|" + sdf.format(d));
			v.set(datetime2 - datetime1);
			context.write(k, v);
			// System.out.println(k.toString()+"===="+v.toString());
		}
	}

	public static class AccessLogStayMrReducer2 extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable(0);

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		DateToNUM.initMap();
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/access.log",
				"hdfs://master:9000/umtmp", "hdfs://master:9000/uvout" };
		Job job = new Job(conf, "stay"); // 设置一个用户定义的job名称
		job.setJarByClass(AccessLogStayMr.class);
		job.setMapperClass(AccessLogStayMrMapper.class); // 为job设置Mapper类
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setReducerClass(AccessLogStayMrReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(Text.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		job.waitForCompletion(true); // 运行job

		Job job2 = new Job(conf, "stay2"); // 设置一个用户定义的job名称
		job2.setJarByClass(AccessLogStayMr.class);
		job2.setMapperClass(AccessLogStayMrMapper2.class); // 为job设置Mapper类
		job2.setReducerClass(AccessLogStayMrReducer2.class); // 为job设置Reducer类
		job2.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job2.setOutputValueClass(LongWritable.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));// 为job设置输出路径
		System.exit(job2.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
