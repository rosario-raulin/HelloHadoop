package de.raulin.rosario.helloHadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public final class TableEntry {

	private final static String[] CSV_KEYS = new String[] { "timestamp",
			"pkey", "hidden", "alat", "along", "avail", "tags", "cname",
			"clat", "clong", "author", "date", "email", "love", "impression",
			"cat", "subcat", "landcode", "name", "feedback", "owner", "phone1",
			"phone2", "plz", "pqr", "service", "street", "updatedBy", "website" };

	private final Map<String, String> values;

	public TableEntry(final String[] values) {
		this.values = new HashMap<String, String>(values.length);
		this.values.put("rowkey", values[0] + values[1]);

		for (int i = 2; i < values.length; ++i) { 
			this.values.put(CSV_KEYS[i-2], values[i]);
		}
	}

	public void putToTable(final HTable table) throws IOException {
		final Put put = new Put(Bytes.toBytes(values.get("rowkey")));

		for (final Map.Entry<String, String> entry : values.entrySet()) {
			put.add(Bytes.toBytes("v"), Bytes.toBytes(entry.getKey()),
					Bytes.toBytes(entry.getValue()));
		}

		table.put(put);
	}
}
