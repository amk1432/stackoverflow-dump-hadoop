package com.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import com.mapper.QandAMapper;
import com.reducer.QandAReducer;

public class QandADriver extends Configured implements Tool {
	private final String inputFile = "hdfs://localhost:54310/user/hduser/projects/input/stackoverflow/qanda/posts.xml";
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set(START_TAG_KEY, "<row");
		conf.set(END_TAG_KEY, "/>");
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		// System.out.println(conf.get(START_TAG_KEY));
		Job job = new Job(conf, "StackOverFlow_Q&A_Per_Month");
		FileInputFormat.addInputPath(job, new Path(inputFile));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setJarByClass(QandADriver.class);
		job.setMapperClass(QandAMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob("MonthlyQuestionandAnswers",
				QandAReducer.class, job);
		return (job.waitForCompletion(true) ? 1 : 0);
	}

	public static void main(String[] args) {

		try {
			int exitcode = ToolRunner.run(new QandADriver(), args);
			System.exit(exitcode);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
