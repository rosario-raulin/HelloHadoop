package de.raulin.rosario.helloHadoop;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public final class BloomFilterTester {

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		if (args.length >= 2) {
			final BloomFilter<String> filter = new BloomFilter<String>();
			final Path inPath = new Path(args[0]);
			final FSDataInputStream in = inPath.getFileSystem(new Configuration()).open(inPath);
			filter.readFields(in);
			
			for (int i = 1; i < args.length; ++i) {
				if (!filter.contains(args[i])) {
					System.out.println("Key not found in filter: " + args[i]);
				} else {
					System.out.println("Key found: " + args[i]);
				}
			}
		} else {
			System.err.println("usage: java BloomFilterTester filter-inpath first-test-key [other-test-keys]");
		}
	}

}
