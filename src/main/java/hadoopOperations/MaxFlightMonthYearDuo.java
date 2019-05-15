package hadoopOperations;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.swing.JOptionPane;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxFlightMonthYearDuo implements Tool {

	private static MaxFlight maxFlight = new MaxFlight();
	private static Logger logger = LoggerFactory.getLogger(MaxFlightMonthYearDuo.class.getName());

	public static class Map extends Mapper<LongWritable, Text, Text, MaxFlight> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException {
			try {
				String[] row = value.toString().split(",");
				if (row.length == 23) {
					Integer year = Integer.parseInt(row[0]);
					Integer month = Integer.parseInt(row[1]);
					if ((2000 <= year) && (year <= 2003) && (1 <= month) && (month <= 12)) {
						maxFlight.setCount(1);
						context.write(new Text(year + "/" + month), maxFlight);
					}
				}
			} catch (NumberFormatException e) {
				logger.debug(e.getMessage(), e);
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static class Combine extends Reducer<Text, MaxFlight, Text, MaxFlight> {
		@Override
		public void reduce(Text key, Iterable<MaxFlight> values, Context context) throws InterruptedException {
			try {
				int count = 0;
				for (MaxFlight value : values) {
					count += value.getCount();
				}
				String[] ym = key.toString().split("/");

				maxFlight.setCount(count);
				maxFlight.setYear(Integer.parseInt(ym[0]));
				maxFlight.setMonth(Integer.parseInt(ym[1]));
				context.write(new Text("key"), maxFlight);
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static class Reduce extends Reducer<Text, MaxFlight, Text, IntWritable> {
		private static int max, year, month;

		@Override
		public void reduce(Text key, Iterable<MaxFlight> values, Context context) throws InterruptedException {
			try {
				max = 0;
				for (MaxFlight value : values) {
					int count = value.getCount();
					if (max < count) {
						max = count;
						year = value.getYear();
						month = value.getMonth();
					}
				}
				context.write(new Text(year + "/" + month), new IntWritable(max));
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		MaxFlightMonthYearDuo maxFlightMonthYearDuo = new MaxFlightMonthYearDuo();
		int code = ToolRunner.run(maxFlightMonthYearDuo, args);
		if (code == 0) {
			JOptionPane.showMessageDialog(null, "Map reduce task completed");
		}
	}

	public int run(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Max Flight Month Year Duo");
		job.setJarByClass(MaxFlightMonthYearDuo.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MaxFlight.class);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fileSystem = (DistributedFileSystem) FileSystem.get(new URI("hdfs://master:9000"), configuration);
		fileSystem.delete(outputPath, true);

		JOptionPane.showMessageDialog(null, "Map reduce task start running");
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}
}
