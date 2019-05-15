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

public class AverageFlightCancellation implements Tool {

	private static Logger logger = LoggerFactory.getLogger(AverageFlightCancellation.class.getName());

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException {
			try {
				String[] row = value.toString().split(",");
				if (row.length == 23) {
					Integer year = Integer.parseInt(row[0]);
					Integer cancellation = Integer.parseInt(row[22]);
					if ((2000 <= year) && (year <= 2003) && ((cancellation == CancellationType.CANCELLED.getId())
							|| cancellation == CancellationType.NOT_CANCELLED.getId())) {
						context.write(new Text(year + ""), new IntWritable(cancellation));
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
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException {
			try {
				int count = 0;
				int cancalled = 0;
				for (IntWritable value : values) {
					count += 1;
					cancalled += value.get();
				}
				double result = cancalled * 1.0 / count;
				context.write(key, new Text(result + " "));
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		AverageFlightCancellation averageFlightCancellation = new AverageFlightCancellation();
		int code = ToolRunner.run(averageFlightCancellation, args);
		if (code == 0) {
			JOptionPane.showMessageDialog(null, "Map reduce task completed");
		}
	}

	public int run(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Average Flight Cancellation");
		job.setJarByClass(AverageFlightCancellation.class);
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
