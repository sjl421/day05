package org.mr.detail.input;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindMaxValue {

	public static class FindMaxValueMapper extends
			Mapper<IntWritable, ArrayWritable, IntWritable, FloatWritable> {

		private final static IntWritable one = new IntWritable(1);

		// private PrintWriter file=null;
		public void map(IntWritable key, ArrayWritable value, Context context)
				throws IOException, InterruptedException {
			FloatWritable[] floatArray = (FloatWritable[]) value.toArray();
			float maxfloat = floatArray[0].get();
			float tmp;
			for (int i = 1; i < floatArray.length; i++) {
				tmp = floatArray[i].get();
				if (tmp > maxfloat) {
					maxfloat = tmp;
				}
			}
			
			context.write(one, new FloatWritable(maxfloat));
		}
	}

	public static  class FindMaxValueReducer extends Reducer<IntWritable, FloatWritable, Text, FloatWritable> {
		 public void reduce(IntWritable key, Iterable<FloatWritable> values, 
                 Context context
                 ) throws IOException, InterruptedException {
			 Iterator it = values.iterator();
			 float maxfloat=0, tmp;
			 if(it.hasNext())
				 maxfloat = ((FloatWritable)(it.next())).get();
			 else
			 {
				 context.write(new Text("Max float value: "), null);
				 return;
			 }
			 //System.out.print(maxfloat + " ");
			 while(it.hasNext())
			 {
				 tmp = ((FloatWritable)(it.next())).get();
				 //System.out.print(tmp + " ");
				 if (tmp > maxfloat) {
					 maxfloat = tmp;
				 }
			 }
			 context.write(new Text("Max float value: "), new FloatWritable(maxfloat));
		 }
		
	}

	/**
	 * @param args
	 * @throws Exception 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		String[] otherArgs = {"hdfs://master:9000/access.log","hdfs://master:9000/uvout"};
		
		conf.set("NumOfValues", "100");
		Job job = new Job(conf, "FindMaxValue");
		// job.setNumReduceTasks(2);

		job.setJarByClass(FindMaxValue.class);
		job.setMapperClass(FindMaxValueMapper.class);
		job.setReducerClass(FindMaxValueReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setInputFormatClass(FindMaxValueInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.out.println(conf.get("mapred.job.tracker"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
