package de.raulin.rosario.helloHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import au.com.bytecode.opencsv.CSVParser;

public final class TableFillingMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final String TABLENAME = "serviceparrot";
	private static final CSVParser PARSER = new CSVParser();
	private final HTable table;

	public TableFillingMapper() throws IOException {
		final Configuration conf = HBaseConfiguration.create();
		this.table = new HTable(conf, TABLENAME);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		final String next = value.toString();
		final String[] values = PARSER.parseLine(next);

		final TableEntry entry = new TableEntry(values);
		entry.putToTable(table);
	}

	public void close() throws IOException {
		table.close();
	}

	public static void main(String[] args) {
		if (args.length >= 2) {
			try {
				final Configuration conf = new Configuration();
				final Job job = new Job(conf, "tableFillingJob");

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setMapperClass(TableFillingMapper.class);

				TextInputFormat.addInputPath(job, new Path(args[0]));
				TextOutputFormat.setOutputPath(job, new Path(args[1]));

				job.setJarByClass(TableFillingMapper.class);
				job.waitForCompletion(true);

			} catch (final IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.err
					.println("usage: java TableFillingMapper input-path output-path");
		}
	}
}
