package com.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class QTop10TagsMapper extends Mapper<LongWritable, Text, Text, Text> {
	public String ss = "";
	String[] splitDate;
	public static final Set<String> top10Tags = new HashSet<String>(
			Arrays.asList(new String[] { "c#", "java", "php", "javascript",
					"jquery", "iphone", ".net", "c++", "asp.net", "android" }));

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ss = value.toString();
		String PostTypeId = "";
		String Date = "";
		String Tags = "";
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
			XPathExpression xpe = xpath.compile("//@PostTypeId");
			XPathExpression xpeTags = xpath.compile("//@Tags");
			NodeList nl = (NodeList) xpe.evaluate(d, XPathConstants.NODESET);
			NodeList nlDate = (NodeList) xpeDate.evaluate(d,
					XPathConstants.NODESET);
			NodeList nlTags = (NodeList) xpeTags.evaluate(d,
					XPathConstants.NODESET);
			PostTypeId = nl.item(0).getNodeValue();
			Date = nlDate.item(0).getNodeValue();
			splitDate = Date.substring(0, 10).split("-");
			if (PostTypeId.equals("1")) {
				if (nlTags.getLength() > 0) {
					Tags = nlTags.item(0).getNodeValue();
					String[] tahsArray = Tags.split("<");
					String delimiter = ",";
					for (String sample : tahsArray) {
						if (top10Tags.contains(sample.split(">")[0])) {
							if(sample.split(">")[0].equals(".net")){
								sb.append("dotnet").append(delimiter);
							}
							else if(sample.split(">")[0].equals("asp.net")){
								sb.append("aspdotnet").append(delimiter);
							}
							else{
								sb.append(sample.split(">")[0]).append(delimiter);
							}
							// delimiter = ",";
						}
					}
					if (!sb.toString().isEmpty()) {
						 //System.out.println("Date ===>>"+splitDate[0] +
						// splitDate[1] +
						// "01"+"---"+"Tags====>>"+sb);
						context.write(new Text(splitDate[0] + splitDate[1]
								+ "01"), new Text(sb.toString()));
					}
				}
			}
		} catch (Exception e) {
			System.out.println("exception " + e);
		}
	}
}
