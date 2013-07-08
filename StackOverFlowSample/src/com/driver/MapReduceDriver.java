package com.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import com.mapper.StackoverflowMapper;
import com.reducer.StackoverflowReducer;

public class MapReduceDriver extends Configured implements Tool {
	private final String inputPath = "hdfs://localhost:54310/user/hduser/projects/input/stackoverflow/badges.xml";
	private final String outputPath = "hdfs://localhost:54310/user/hduser/projects/output/stackoverflow";
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 //Configuration conf1 = getConf();
		conf.set(START_TAG_KEY, "<row");
		conf.set(END_TAG_KEY, "/>");
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		// System.out.println(conf.get(START_TAG_KEY));
		Job job = new Job(conf, "jobName");
		job.setInputFormatClass(XmlInputFormat.class);
		job.setJarByClass(MapReduceDriver.class);
		job.setMapperClass(StackoverflowMapper.class);
		job.setReducerClass(StackoverflowReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return (job.waitForCompletion(true) ? 1 : 0);

	}

	public static void main(String[] args) {

		try {
			int exitcode = ToolRunner.run(new MapReduceDriver(), args);
			System.exit(exitcode);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
