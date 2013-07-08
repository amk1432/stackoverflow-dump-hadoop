package com.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class QandAMapper extends Mapper<LongWritable, Text, Text, Text> {
	public String ss = "";
	String[] splitDate;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ss = value.toString();
		String PostTypeId = "";
		String Date = "";
		ByteArrayInputStream bis = new ByteArrayInputStream(ss.getBytes());
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document d = builder.parse(bis);
			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();
			XPathExpression xpeDate = xpath.compile("//@CreationDate");
			XPathExpression xpe = xpath.compile("//@PostTypeId");
			NodeList nl = (NodeList) xpe.evaluate(d, XPathConstants.NODESET);
			NodeList nlDate = (NodeList) xpeDate.evaluate(d,
					XPathConstants.NODESET);
			PostTypeId = nl.item(0).getNodeValue();
			Date = nlDate.item(0).getNodeValue();
			splitDate = Date.substring(0, 10).split("-");
			
		} catch (Exception e) {
			System.out.println("exception " + e);
		}
		context.write(new Text(splitDate[0] + splitDate[1] + "01"),
				new Text(PostTypeId));
	}

}
