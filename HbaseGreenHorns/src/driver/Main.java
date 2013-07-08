package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import mapper.Mapper;
import reducer.Reducer;

public class Main extends Configured implements Tool {
	private final String inputFile = "hdfs://localhost:54310/user/hduser/projects/input/hbasegreenhorns/cars.csv";
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "jobName");
		job.setJarByClass(Main.class);
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(Reducer.class);
		return (job.waitForCompletion(true) ? 1 : 0);

	}

	public static void main(String[] args) {

		try {
			int exitcode = ToolRunner.run(new Main(), args);
			System.exit(exitcode);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
