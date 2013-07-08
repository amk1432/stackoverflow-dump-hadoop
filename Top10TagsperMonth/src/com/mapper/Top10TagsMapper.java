package com.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class Top10TagsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String ss = value.toString();
		String Date = "";
		String Tags = "";
		String[] splitDate;
		StringBuilder sb = new StringBuilder();
		ByteArrayInputStream bis = new ByteArrayInputStream(ss.getBytes());
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document d = builder.parse(bis);
			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();
			XPathExpression xpeDate = xpath.compile("//@CreationDate");
			XPathExpression xpeTags = xpath.compile("//@Tags");
			NodeList nlDate = (NodeList) xpeDate.evaluate(d,
					XPathConstants.NODESET);
			NodeList nlTags = (NodeList) xpeTags.evaluate(d,
					XPathConstants.NODESET);
			Date = nlDate.item(0).getNodeValue();
			splitDate = Date.substring(0, 10).split("-");
			if (nlTags.getLength() > 0) {
				Tags = nlTags.item(0).getNodeValue();
				String[] tahsArray = Tags.split("<");
				String delimiter = "";
				for (String sample : tahsArray) {
						sb.append(sample.split(">")[0]).append(delimiter);
						delimiter = ",";

				}
				// System.out.println("Date ===>>"+splitDate[0] + splitDate[1] +
				//"01"+"---"+"Tags====>>"+sb);
				context.write(new Text("100000"),
						new Text(sb.toString()));
			}

		} catch (Exception e) {
			System.out.println("exception " + e);
		}
		
	}
}
