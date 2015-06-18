package org.mr.detail.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FindMaxValueInputFormat extends
		InputFormat<IntWritable, ArrayWritable> {
	public static float[] floatvalues;

	@Override
	public RecordReader<IntWritable, ArrayWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FindMaxValueRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		int NumOfValues = context.getConfiguration().getInt("NumOfValues", 100);
		floatvalues = new float[NumOfValues];
		Random rand = new Random();
		for (int i = 0; i < NumOfValues; i++) {
			floatvalues[i] = rand.nextFloat();
		}
		int NumSplits = context.getConfiguration()
				.getInt("mapred.map.tasks", 2);
		int beg = 0;
		int length = (int) Math.floor(NumOfValues / NumSplits);
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		int end = length - 1;
		for (int i = 0; i < NumSplits - 1; i++) {
			FindMaxValueInputSplit split = new FindMaxValueInputSplit(beg, end);
			splits.add(split);
			beg = end + 1;
			end = beg + length - 1;
		}
		FindMaxValueInputSplit split = new FindMaxValueInputSplit(beg,
				NumOfValues - 1);
		splits.add(split);
		return splits;
	}
}
