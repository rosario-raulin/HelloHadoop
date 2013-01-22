package de.raulin.rosario.helloHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class ZipCounter {

	private final static String TABLENAME = "serviceparrot";

	private static class ZipCounterMapper extends
			TableMapper<Text, IntWritable> {
		private static IntWritable ONE = new IntWritable(1);

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			byte[] plz = value.getValue(Bytes.toBytes("v"),
					Bytes.toBytes("plz"));
			context.write(new Text(plz), ONE);
		}
	}

	private static class ZipCounterReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) {
		if (args.length >= 1) {
			try {
				final Configuration conf = new Configuration();
				final Job job = new Job(conf, "zipCounterJob");

				job.setJarByClass(ZipCounter.class);
				job.setMapperClass(ZipCounter.ZipCounterMapper.class);
				job.setReducerClass(ZipCounter.ZipCounterReducer.class);

				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);

				job.setInputFormatClass(TableInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				TextOutputFormat.setOutputPath(job, new Path(args[0]));

				TableMapReduceUtil.initTableMapperJob(TABLENAME, new Scan(),
						ZipCounter.ZipCounterMapper.class, Text.class,
						IntWritable.class, job);

				job.waitForCompletion(true);
			} catch (final IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.err.println("usage: java ZipCounter output-path");
		}
	}
}
