package com.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TempMapper extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
	private Text mapperKey = new Text();
	private Text mapperValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valueSplitted = value.toString().split(",");
		String state = valueSplitted[0];
		String temp = valueSplitted[2];
		mapperKey.set(state);
		mapperValue.set(temp);
		context.write(mapperKey, mapperValue);
	}
}
