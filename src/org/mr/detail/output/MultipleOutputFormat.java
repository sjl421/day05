package org.mr.detail.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>
		extends TextOutputFormat<K, V> {
	private MultiRecordWriter writer = null;

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		if (writer == null) {
			writer = new MultiRecordWriter(job, getTaskOutputPath(job));
		}
		return writer;
	}

	private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
		Path workPath = null;
		OutputCommitter committer = super.getOutputCommitter(conf);
		if (committer instanceof FileOutputCommitter) {
			workPath = ((FileOutputCommitter) committer).getWorkPath();
		} else {
			Path outputPath = super.getOutputPath(conf);
			if (outputPath == null) {
				throw new IOException("Undefined job output-path");
			}
			workPath = outputPath;
		}
		return workPath;
	}

	/** 通过key, value, conf来确定输出文件名（含扩展名） */

	public class MultiRecordWriter extends RecordWriter<K, V> {
		/** RecordWriter的缓存 */
		private HashMap<String, RecordWriter<K, V>> recordWriters = null;
		private TaskAttemptContext job = null;
		/** 输出目录 */
		private Path workPath = null;

		public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
			super();
			this.job = job;
			this.workPath = workPath;
			recordWriters = new HashMap<String, RecordWriter<K, V>>();
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			Iterator<RecordWriter<K, V>> values = this.recordWriters.values()
					.iterator();
			while (values.hasNext()) {
				values.next().close(context);
			}
			this.recordWriters.clear();
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			// 得到输出文件名
			String baseName = generateFileNameForKeyValue(key, value,
					job.getConfiguration());
			RecordWriter<K, V> rw = this.recordWriters.get(baseName);
			if (rw == null) {
				String keyValueSeparator = ",";
				Path file = new Path(workPath, baseName);
				FSDataOutputStream fileOut = file.getFileSystem(job.getConfiguration()).create(file,false);
				rw =  new LineRecordWriter<K, V>(fileOut,keyValueSeparator);
				this.recordWriters.put(baseName, rw);
			}
			rw.write(key, value);
		}

		private String generateFileNameForKeyValue(K key, V value,
				Configuration conf) {
			char c = key.toString().toLowerCase().charAt(0);
			if (c >= 'a' && c <= 'z') {
				return c + ".txt";
			}
			return "other.txt";
		}
	}
}
