package org.mr.detail.input;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FindMaxValueRecordReader extends
		RecordReader<IntWritable, ArrayWritable> {

	private int m_End;
	private int m_Index;
	private int m_Start;
	private IntWritable key = null;
	private ArrayWritable value = null;
	private FindMaxValueInputSplit fmvsplit = null;

	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public ArrayWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (this.m_Index == this.m_End) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (this.m_Index - this.m_Start)
					/ (float) (this.m_End - this.m_Start));
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		fmvsplit = (FindMaxValueInputSplit) split;
		this.m_Start = fmvsplit.getM_StartIndex();
		this.m_End = fmvsplit.getM_EndIndex();
		this.m_Index = this.m_Start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new IntWritable();
		}
		if (value == null) {
			value = new ArrayWritable(FloatWritable.class);
		}
		if (m_Index <= m_End) {
			key.set(m_Index);
			value = fmvsplit.getM_FloatArray();
			m_Index = m_End + 1;
			return true;
		} else
			return false;
	}

}
