package org.mr.detail.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FindMaxValueInputSplit extends InputSplit implements Writable {

	private int m_StartIndex;
	private int m_EndIndex;
	private ArrayWritable m_FloatArray = new ArrayWritable(FloatWritable.class);

	public FindMaxValueInputSplit() {
	}
	public FindMaxValueInputSplit(int start, int end) {
		m_StartIndex = start;
		m_EndIndex = end;
		int len = m_EndIndex - m_StartIndex + 1;
		int Index = m_StartIndex;
		FloatWritable[] result = new FloatWritable[len];
		for (int i = 0; i < len; i++) {
			float f = FindMaxValueInputFormat.floatvalues[Index];
			FloatWritable fw = new FloatWritable();
			fw.set(f);
			result[i] = fw;
			Index++;
		}
		m_FloatArray.set(result);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return (this.m_EndIndex - this.m_StartIndex + 1);
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] { "hadoop-2", "hadoop-1" };
	}

	public int getM_StartIndex() {
		return m_StartIndex;
	}

	public int getM_EndIndex() {
		return m_EndIndex;
	}

	public void readFields(DataInput in) throws IOException {
		this.m_StartIndex = in.readInt();
		this.m_EndIndex = in.readInt();
		this.m_FloatArray.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.m_StartIndex);
		out.writeInt(this.m_EndIndex);
		this.m_FloatArray.write(out);
	}

	public ArrayWritable getM_FloatArray() {
		return m_FloatArray;
	}

}
