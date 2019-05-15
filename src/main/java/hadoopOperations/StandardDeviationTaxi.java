package hadoopOperations;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

public class StandardDeviationTaxi implements Tool {

	private static Logger logger = LoggerFactory.getLogger(StandardDeviationTaxi.class.getName());

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException {
			try {
				String[] row = value.toString().split(",");
				if (row.length == 23) {
					Integer year = Integer.parseInt(row[0]);
					Integer day = Integer.parseInt(row[3]);
					Integer taxi = Integer.parseInt(row[19]);
					if (((day == Day.SATURDAY.getId()) || (day == Day.SUNDAY.getId())) && (taxi >= 0)) {
						context.write(new Text(String.valueOf(year)), new IntWritable(taxi));
					}
				}
			} catch (NumberFormatException e) {
				logger.debug(e.getMessage(), e);
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		List<IntWritable> cache;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException {
			cache = new ArrayList<IntWritable>();

			try {
				int count = 0;
				int totalTaxi = 0;
				Iterator<IntWritable> iter = values.iterator();
				while (iter.hasNext()) {
					IntWritable value = iter.next();
					totalTaxi += value.get();
					count += 1;
					cache.add(value);
				}
				double meanTaxi = totalTaxi * 1.0 / count;

				double difference = 0;
				for (IntWritable value : cache) {
					difference += Math.pow((value.get() - meanTaxi), 2);
				}

				double std = Math.sqrt(difference * 1.0 / (count - 1));
				context.write(new Text(key), new Text(std + ""));
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		StandardDeviationTaxi standardDeviationTaxi = new StandardDeviationTaxi();
		int code = ToolRunner.run(standardDeviationTaxi, args);
		if (code == 0) {
			JOptionPane.showMessageDialog(null, "Map reduce task completed");
		}
	}

	public int run(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Standard Deviation Taxi in");
		job.setJarByClass(StandardDeviationTaxi.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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