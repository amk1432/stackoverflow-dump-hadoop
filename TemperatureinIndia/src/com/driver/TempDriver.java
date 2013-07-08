package com.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.mapper.TempMapper;
import com.reducer.TempReducer;

public class TempDriver extends Configured implements Tool {
	private final String inputFile = "hdfs://localhost:54310/user/hduser/projects/input/tempinindia/temp1.csv";
	private final String outputPath = "hdfs://localhost:54310/user/hduser/projects/output/stackoverflow";

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Temperature In India");
		
		job.setJarByClass(TempDriver.class);
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return (job.waitForCompletion(true) ? 1 : 0);

	}

	public static void main(String[] args) {

		try {
			int exitcode = ToolRunner.run(new TempDriver(), args);
			System.exit(exitcode);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
