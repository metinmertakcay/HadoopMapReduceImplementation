package hadoopOperations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MaxFlight implements Writable {

	private Integer count;
	private Integer year;
	private Integer month;

	public MaxFlight() {
		count = 0;
		year = 0;
		month = 0;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeInt(year);
		out.writeInt(month);
	}

	public void readFields(DataInput in) throws IOException {
		count = new Integer(in.readInt());
		year = new Integer(in.readInt());
		month = new Integer(in.readInt());
	}
}