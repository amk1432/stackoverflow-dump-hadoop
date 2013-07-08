package com.reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;

public class TempReducer extends
		org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int temp = 0;
		int avgtemp = 0;
		String state = key.toString();
		for (Text value : values) {
			temp = temp + Integer.parseInt(value.toString());
		}
		avgtemp = temp / 3;
		System.out.println("State=" + state + "    Average Tempreature="
				+ avgtemp);
	}
}
