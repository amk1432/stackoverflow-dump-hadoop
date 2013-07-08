package com.reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Top10TagsReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	Context con = null;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Integer> tags = new HashMap<String, Integer>();
		ValueComparator bvc = new ValueComparator(tags);
		TreeMap<String, Integer> sorted_map = new TreeMap<String, Integer>(bvc);
		con = context;
		String date = key.toString();
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {

			Text text = (Text) itr.next();
			String[] tagList = text.toString().split(",");
			for (int i = 0; i < tagList.length; i++) {
				if (i == tagList.length - 1)
					break;
				if (tags.containsKey(tagList[i])) {
					int sample = (int) tags.get(tagList[i]) + 1;
					tags.put(tagList[i], sample);
				} else {
					tags.put(tagList[i], 1);
				}
			}
		}
		sorted_map.putAll(tags);
		System.out.println(sorted_map);
		Iterator it = sorted_map.entrySet().iterator();
		int count = 0;
		byte[] row = Bytes.toBytes(Long.parseLong(date));
		byte[] col = Bytes.toBytes("Tags");
		while (it.hasNext()) {
			if (count == 10)
				break;
			else {
				Map.Entry pairs = (Map.Entry) it.next();
				System.out.println(pairs.getKey() + " = " + pairs.getValue());
				byte[] qaul = Bytes.toBytes(pairs.getKey().toString());
				byte[] value = Bytes.toBytes(Integer.parseInt(pairs.getValue()
						.toString()));
				putRow(row, col, qaul, value);
				it.remove(); // avoids a ConcurrentModificationException
				count++;
			}
		}
	}

	public void putRow(byte[] row, byte[] column, byte[] qaul, byte[] value)
			throws IOException, InterruptedException {
		Put put = new Put(row);
		put.add(column, qaul, value);
		con.write(
				new ImmutableBytesWritable(Bytes.toBytes("Top10TagsperMonth")),
				put);
	}
}

class ValueComparator implements Comparator<String> {
	Map<String, Integer> base;

	public ValueComparator(Map<String, Integer> base) {
		this.base = base;
	}

	// Note: this comparator imposes orderings that are inconsistent with
	// equals.
	public int compare(String a, String b) {
		if (base.get(a) >= base.get(b)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}
}
