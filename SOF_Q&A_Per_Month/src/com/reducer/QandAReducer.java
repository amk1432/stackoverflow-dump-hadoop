package com.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class QandAReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	public Map<String, Integer> question = new HashMap<String,Integer>();
	public Map<String, Integer> answer = new HashMap<String,Integer>();
	String columns = "Numbers:Questions,Numbers:Answers";
	String[] columnList = columns.split(",");

	Context con = null;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		con = context;
		long[] columnsValue={0,0};
		String date = key.toString();
		/*
		
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			Text text = (Text) itr.next();
			
		}*/
		/*for (int k = 0; k < 2; k++) {
			String[] columnQual = columnList[k].split(":");
			byte[] col = Bytes.toBytes(columnQual[0]);
			byte[] qaul = Bytes.toBytes(columnQual[1]);
			byte[] value = Bytes.toBytes(Long.parseLong(columnsValue[k]));
			//putRow(row, col, qaul, value);
		}*/
		int qCount=0,aCount=0;
		
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			
		
			Text text = (Text) itr.next();
			
			if(Integer.parseInt(text.toString()) == 1)
				qCount++;
			else
				aCount++;
			
		}
		columnsValue[0] = qCount;
		columnsValue[1] = aCount;
		
		byte[] row = Bytes.toBytes(Long.parseLong(date));
		for (int k = 0; k < 2; k++) {
			String[] columnQual = columnList[k].split(":");
			byte[] col = Bytes.toBytes(columnQual[0]);
			byte[] qaul = Bytes.toBytes(columnQual[1]);
			byte[] value = Bytes.toBytes(columnsValue[k]);
			putRow(row, col, qaul, value);
		}
		System.out.println("Date====>>"+date+"Question Count======>>>>"+qCount+"Answer Count=======>>>>"+aCount);
	}
	
	
	public void putRow(byte[] row, byte[] column, byte[] qaul, byte[] value)
			throws IOException, InterruptedException {
		Put put = new Put(row);
		put.add(column, qaul, value);
		con.write(
				new ImmutableBytesWritable(Bytes
						.toBytes("MonthlyQuestionandAnswers")), put);
	}
}
