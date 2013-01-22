package de.raulin.rosario.helloHadoop;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;

public final class MD5Cache implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Set<byte[]> data;

	public MD5Cache() {
		this.data = new HashSet<byte[]>();
	}

	public void add(final Iterable<String> keys) {
		try {
			final MessageDigest digest = MessageDigest.getInstance("MD5");
			for (final String key : keys) {
				digest.reset();
				digest.update(key.getBytes());
				data.add(digest.digest());
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} 
	}

	public boolean contains(String key) {
		try {
			final MessageDigest digest = MessageDigest.getInstance("MD5"); 
			digest.reset();
			digest.update(key.getBytes());
			final byte[] dig = digest.digest();
			return data.contains(dig);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return false;
		}
	}

	private static MD5Cache getMD5Cache(final String inPath) throws IOException {
		final CSVReader reader = new CSVReader(new FileReader(inPath));

		String[] nextLine;
		final List<String> locationNames = new LinkedList<String>();
		while ((nextLine = reader.readNext()) != null) {
			final String next = nextLine[20];
			locationNames.add(next);
		}
		reader.close();
		final MD5Cache cache = new MD5Cache();
		cache.add(locationNames);
		return cache;
	}

	public static void main(String[] args) throws IOException {
		if (args.length >= 2) {
			final MD5Cache cache = getMD5Cache(args[0]);
			final ObjectOutputStream out = new ObjectOutputStream(
					new FileOutputStream(args[1]));
			out.writeObject(cache);
			out.close();
		}
	}
}
