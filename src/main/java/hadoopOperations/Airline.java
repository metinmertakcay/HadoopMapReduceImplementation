package hadoopOperations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Airline implements Writable {

	private Text name;
	private IntWritable count;

	public Airline() {
		name = new Text();
		count = new IntWritable(0);
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		name.write(out);
		count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		count.readFields(in);
	}
}