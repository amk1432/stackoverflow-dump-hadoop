package com.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import com.mapper.QTop10TagsMapper;
import com.reducer.QTop10TagsReducer;

public class QTop10TagsDriver extends Configured implements Tool {
	private final String inputFile = "hdfs://localhost:54310/user/hduser/projects/input/stackoverflow/qanda/posts.xml";
	// private final String outputPath =
	// "hdfs://localhost:54310/user/hduser/projects/output/stackoverflow";
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
		Job job = new Job(conf, "StackOverFlow_Q&A_Per_Month for top 10 tags");
		FileInputFormat.addInputPath(job, new Path(inputFile));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setJarByClass(QTop10TagsDriver.class);
		job.setMapperClass(QTop10TagsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob("Questions4Top10Tags",
				QTop10TagsReducer.class, job);
		// FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return (job.waitForCompletion(true) ? 1 : 0);
	}

	public static void main(String[] args) {

		try {
			int exitcode = ToolRunner.run(new QTop10TagsDriver(), args);
			System.exit(exitcode);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
