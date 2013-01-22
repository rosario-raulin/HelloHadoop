package de.raulin.rosario.helloHadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import au.com.bytecode.opencsv.CSVParser;

public final class BloomFilterMR {

	public static class BloomFilterMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, BloomFilter<String>> {
		private static final CSVParser PARSER = new CSVParser();

		private final BloomFilter<String> filter = new BloomFilter<String>();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, BloomFilter<String>> collector,
				Reporter reporter) throws IOException {

			final String next = value.toString();
			final String locationName = PARSER.parseLine(next)[20];

			filter.add(locationName);
			collector.collect(new Text("myfilter"), filter);
		}
	}

	public static class BloomFilterReducer extends MapReduceBase implements
			Reducer<Text, BloomFilter<String>, Text, Text> {

		private JobConf job;
		private final BloomFilter<String> bf = new BloomFilter<String>();

		@Override
		public void configure(JobConf job) {
			this.job = job;
		}

		@Override
		public void reduce(Text key, Iterator<BloomFilter<String>> values,
				OutputCollector<Text, Text> oc, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				bf.union(values.next());
			}
		}

		@Override
		public void close() throws IOException {
			final Path file = new Path(job.get("mapred.output.dir")
					+ "/bloomfilter");
			final FSDataOutputStream out = file.getFileSystem(job).create(file);
			bf.write(out);
			out.close();
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length >= 2) {
			final Configuration conf = new Configuration();
			final JobConf job = new JobConf(conf, BloomFilterMR.class);

			final Path in = new Path(args[0]);
			final Path out = new Path(args[1]);
			FileInputFormat.setInputPaths(job, in);
			FileOutputFormat.setOutputPath(job, out);

			job.setJobName("bloomFilterGenerator");
			job.setMapperClass(BloomFilterMR.BloomFilterMapper.class);
			job.setReducerClass(BloomFilterMR.BloomFilterReducer.class);
			job.setNumReduceTasks(1);

			job.setInputFormat(TextInputFormat.class);
			job.setOutputFormat(NullOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(BloomFilter.class);

			JobClient.runJob(job);
		} else {
			System.err.println("usage: java BloomFilterMR in-path out-path");
		}
	}
}
