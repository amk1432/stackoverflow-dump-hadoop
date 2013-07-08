package com.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class StackoverflowMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	public ArrayList<String> output = new ArrayList<String>();
	public String ss = "";

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ss = value.toString();
		String NameAttr = "";
		ByteArrayInputStream bis = new ByteArrayInputStream(ss.getBytes());
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document d = builder.parse(bis);
			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();
			XPathExpression xpe = xpath.compile("//@Name");
			NodeList nl = (NodeList) xpe.evaluate(d, XPathConstants.NODESET);

			for (int i = 0; i < nl.getLength(); i++) {
				NameAttr = nl.item(i).getNodeValue();
			}
		} catch (Exception e) {
			System.out.println("exception " + e);
		}
		// System.out.println("xml key input....................." + key);
		context.write(new IntWritable(), new Text(NameAttr));
		// super.map(key, value, context);
	}
}
