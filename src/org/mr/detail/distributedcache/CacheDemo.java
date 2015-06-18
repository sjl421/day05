package org.mr.detail.distributedcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CacheDemo {

	public static void UseDistributedCacheBySymbolicLink(Context context) throws Exception {
		
//		Path filePath[]=DistributedCache.getLocalCacheFiles(context.getConfiguration());
//		for(Path p:filePath){
//			System.out.println(p.getName());
//		}
		FileReader reader = new FileReader("aaa.log");
		BufferedReader br = new BufferedReader(reader);
		String s = null;
		while ((s = br.readLine()) != null) {
			System.out.println(s);
		}
		br.close();
		reader.close();
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		protected void setup(Context context) throws IOException,InterruptedException {
			System.out.println("Now, use the distributed cache and syslink");
			try {
				UseDistributedCacheBySymbolicLink(context);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
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
		conf.set("mapred.create.symlink", "yes");
		String[] otherArgs = {"hdfs://master:9000/test/access.log","hdfs://master:9000/uvout"};
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI("hdfs://master:9000/test/access.log#aaa.log"), conf);
		Job job = new Job(conf, "CacheDemo");
		job.setJarByClass(CacheDemo.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}