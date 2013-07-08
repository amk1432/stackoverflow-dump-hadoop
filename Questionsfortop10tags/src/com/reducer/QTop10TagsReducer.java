package com.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class QTop10TagsReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	Context con = null;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Integer> tags = new HashMap<String, Integer>();
		tags.put("c#", 0);
		tags.put("java", 0);
		tags.put("php", 0);
		tags.put("javascript", 0);
		tags.put("jquery", 0);
		tags.put("iphone", 0);
		tags.put("dotnet", 0);
		tags.put("c++", 0);
		tags.put("aspdotnet", 0);
		tags.put("android", 0);
		
		con = context;
		String date = key.toString();
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			Text text = (Text) itr.next();
			String[] tagList = text.toString().split(",");
			for (int i = 0; i < tagList.length; i++) {
				/*if (i == tagList.length - 1)
					break;*/
				if (tags.containsKey(tagList[i])) {
					int sample = (int) tags.get(tagList[i]) + 1;
					tags.put(tagList[i], sample);
				} else {
					tags.put(tagList[i], 1);
				}
			}
		}
		System.out.println(date);
		System.out.println(tags);
		Iterator it = tags.entrySet().iterator();
		byte[] row = Bytes.toBytes(Long.parseLong(date));
		byte[] col = Bytes.toBytes("Tags");
		while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry) it.next();
				//System.out.println(pairs.getKey() + " = " + pairs.getValue());
				byte[] qaul = Bytes.toBytes(pairs.getKey().toString());
				byte[] value = Bytes.toBytes(Integer.parseInt(pairs.getValue()
						.toString()));
				putRow(row, col, qaul, value);
				
				it.remove(); // avoids a ConcurrentModificationException
		}
		tags.clear();
	}

	public void putRow(byte[] row, byte[] column, byte[] qaul, byte[] value)
			throws IOException, InterruptedException {
		Put put = new Put(row);
		put.add(column, qaul, value);
		con.write(
				new ImmutableBytesWritable(Bytes.toBytes("Questions4Top10Tags")),
				put);
	}
}