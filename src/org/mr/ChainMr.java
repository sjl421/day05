package org.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;

public class ChainMr {
	private static class Mapper1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String texts[] = value.toString().split(",");
			if (texts[1] != null && texts[1].length() > 0) {
				int count = Integer.parseInt(texts[1]);
				if (count > 10000) {
					return;
				} else {
					output.collect(new Text(texts[0]), new Text(texts[1]));

				}
			}
		}
	}
	private static class Mapper2 extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int count = Integer.parseInt(value.toString());
			if (count >= 100) {
				return;
			} else {
				output.collect(key, value);
			}

		}
	}
	private static class Reducer3 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				Text t = values.next();
				sum += Integer.parseInt(t.toString());
			}
			// 旧API的集合，不支持foreach迭代
			// for(Text t:values){
			// sum+=Integer.parseInt(t.toString());
			// }
			output.collect(key, new Text(sum + ""));
		}

	}
	private static class Mapper4 extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int len = key.toString().trim().length();
			if (len >= 3) {
				return;
			} else {
				output.collect(key, value);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		String[] otherArgs = { "hdfs://master:9000/test1/c.txt","hdfs://master:9000/out/" };
		// Job job=new Job(conf,"myjoin");
		JobConf conf = new JobConf(ChainMr.class);
		conf.setJobName("test");
		conf.setJarByClass(ChainMr.class);
		// Job job=new Job(conf, "test");
		// job.setJarByClass(ChainMr.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);
		// Map1的过滤
		JobConf map1 = new JobConf(false);
		ChainMapper.addMapper(conf, Mapper1.class, LongWritable.class,
				Text.class, Text.class, Text.class, false, map1);
		// Map2的过滤
		JobConf map2 = new JobConf(false);
		ChainMapper.addMapper(conf, Mapper2.class, Text.class, Text.class,
				Text.class, Text.class, false, map2);
		// 设置Reduce
		JobConf recduceFinallyConf = new JobConf(false);
		ChainReducer.setReducer(conf, Reducer3.class, Text.class, Text.class,
				Text.class, Text.class, false, recduceFinallyConf);
		// Reduce过后的Mapper过滤
		JobConf reduce1 = new JobConf(false);
		ChainReducer.addMapper(conf, Mapper4.class, Text.class, Text.class,
				Text.class, Text.class, true, reduce1);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.addInputPath(conf, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));
		// System.exit(conf.waitForCompletion(true)?0:1);
		JobClient.runJob(conf);

	}

}