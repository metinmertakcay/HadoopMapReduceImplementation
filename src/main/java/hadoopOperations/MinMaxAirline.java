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

public class MinMaxAirline implements Tool {

	private static Text nameMin = new Text();
	private static Text nameMax = new Text();
	private static Airline airline = new Airline();
	private static Logger logger = LoggerFactory.getLogger(MinMaxAirline.class.getName());

	public static class Map extends Mapper<LongWritable, Text, Text, Airline> {
		private static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException {
			try {
				String[] row = value.toString().split(",");
				if (row.length == 23) {
					String airlineName = row[8];
					if ((airlineName != null) && (airlineName.length() == 2)) {
						airline.setCount(one);
						value.set(new Text(airlineName));
						context.write(value, airline);
					}
				}
			} catch (NumberFormatException e) {
				logger.debug(e.getMessage(), e);
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static class Combine extends Reducer<Text, Airline, Text, Airline> {
		@Override
		public void reduce(Text key, Iterable<Airline> values, Context context) throws InterruptedException {
			try {
				int count = 0;
				for (Airline value : values) {
					count += value.getCount().get();
				}

				airline.setName(key);
				airline.setCount(new IntWritable(count));
				context.write(new Text("key"), airline);
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Airline, Text, IntWritable> {
		private static int min, max;

		@Override
		public void reduce(Text key, Iterable<Airline> values, Context context) throws InterruptedException {
			try {
				max = Integer.MIN_VALUE;
				min = Integer.MAX_VALUE;
				for (Airline value : values) {
					int count = value.getCount().get();
					if (max < count) {
						max = count;
						nameMax.set(value.getName());
					}
					if (count < min) {
						min = count;
						nameMin.set(value.getName());
					}
				}
				context.write(nameMin, new IntWritable(min));
				context.write(nameMax, new IntWritable(max));
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		MinMaxAirline minMaxAirline = new MinMaxAirline();
		int code = ToolRunner.run(minMaxAirline, args);
		if (code == 0) {
			JOptionPane.showMessageDialog(null, "Map reduce task completed");
		}
	}

	public int run(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Min Max Airline");
		job.setJarByClass(MinMaxAirline.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Airline.class);

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