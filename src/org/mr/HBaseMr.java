package org.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HBaseMr {
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}
	public static final String tableName = "word";
	public static final String colf = "content";
	public static final String col = "info";
	public static final String tableName2 = "stat";
	/**
	 * TableMapper<Text,IntWritable> Text:输出的key类型，IntWritable：输出的value类型
	 */
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);
		private static Text word = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			// 表里面只有一个列族，所以我就直接获取每一行的值
			String words = Bytes.toString(value.getValue(Bytes.toBytes(colf), Bytes.toBytes(col)));
			String itr[] = words.toString().split("");
			for (int i = 0; i < itr.length; i++) {
				word.set(itr[i]);
				context.write(word, one);
			}
		}
	}
	/**
	 * TableReducer<Text,IntWritable>
	 * Text:输入的key类型，IntWritable：输入的value类型，ImmutableBytesWritable：输出类型
	 */
	public static class MyReducer extends
			TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			// 添加一行记录，每一个单词作为行键
			Put put = new Put(Bytes.toBytes(key.toString()));
			// 在列族content中添加一个列info,赋值为每个单词出现的次数
			// String.valueOf(sum)先将数字转化为字符串，否则存到数据库后会变成\x00\x00\x00\x这种形式
			// 然后再转二进制存到hbase。
			put.add(Bytes.toBytes(colf), Bytes.toBytes(col),
					Bytes.toBytes(String.valueOf(sum)));
			context.write(
					new ImmutableBytesWritable(Bytes.toBytes(key.toString())),
					put);
		}
	}

	public static void initTB() {
		HTable table=null;
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(config);
			if (admin.tableExists(tableName)||admin.tableExists(tableName2)) {
				System.out.println("table is already exists!");
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				admin.disableTable(tableName2);
				admin.deleteTable(tableName2);
				System.exit(0);
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(colf);
				desc.addFamily(family);
				admin.createTable(desc);
				
				HTableDescriptor desc2 = new HTableDescriptor(tableName2);
				HColumnDescriptor family2 = new HColumnDescriptor(colf);
				desc2.addFamily(family2);
				admin.createTable(desc2);
				
				table = new HTable(config,tableName);
				table.setAutoFlush(false);
				table.setWriteBufferSize(5);
				
				List<Put> lp = new ArrayList<Put>();
				
				Put p1 = new Put(Bytes.toBytes("1"));
				p1.add(colf.getBytes(), col.getBytes(),
						("The Apache Hadoop software library is a framework")
								.getBytes());
				lp.add(p1);
				
				Put p2 = new Put(Bytes.toBytes("2"));
				p2.add(colf.getBytes(),
						col.getBytes(),
						("The common utilities that support the other Hadoop modules")
								.getBytes());
				lp.add(p2);
				
				Put p3 = new Put(Bytes.toBytes("3"));
				p3.add(colf.getBytes(), col.getBytes(),
						("Hadoop by reading the documentation").getBytes());
				lp.add(p3);
				
				Put p4 = new Put(Bytes.toBytes("4"));
				p4.add(colf.getBytes(), col.getBytes(),
						("Hadoop from the release page").getBytes());
				lp.add(p4);
				
				Put p5 = new Put(Bytes.toBytes("5"));
				p5.add(colf.getBytes(), col.getBytes(),
						("Hadoop on the mailing list").getBytes());
				lp.add(p5);
				
				table.put(lp);
				table.flushCommits();
				
				
				lp.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(table!=null){
					table.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		initTB();
		
		Job job = new Job(config, "HBaseMr");
		job.setJarByClass(HBaseMr.class);
		Scan scan = new Scan();
		// 指定要查询的列族
		scan.addColumn(Bytes.toBytes(colf), Bytes.toBytes(col));
		// 指定Mapper读取的表为word
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MyMapper.class,
				Text.class, IntWritable.class, job);
		// 指定Reducer写入的表为stat
		TableMapReduceUtil.initTableReducerJob(tableName2, MyReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}