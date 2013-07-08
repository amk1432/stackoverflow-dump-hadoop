package com.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StackoverflowReducer extends
		Reducer<IntWritable, Text, Text, Text> {
	public Map<String, Integer> mMap = new HashMap<String, Integer>();

	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		String s = "";
		Iterator<Text> itr = arg1.iterator();
		while (itr.hasNext()) {
			Text text = (Text) itr.next();
			s = text.toString();
			if (mMap.containsKey(s)) {
				int sample = (int) mMap.get(s) + 1;
				mMap.put(s, sample);
			} else {
				mMap.put(s, 1);
			}
		}
		for (Map.Entry<String, Integer> entry : mMap.entrySet()) {
			arg2.write(new Text(entry.getKey()), new Text(entry.getValue()
					.toString()));
			System.out.println("Key = " + entry.getKey() + ", Value = "
					+ entry.getValue());
		}
	}
}
