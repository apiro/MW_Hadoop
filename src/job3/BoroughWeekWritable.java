package job3;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class BoroughWeekWritable implements WritableComparable<BoroughWeekWritable>{
	private Text borough;
	private Text week;

	public BoroughWeekWritable() {
		this.borough = new Text();
		this.week = new Text();
	}

	public BoroughWeekWritable(String borough, String week) {
		this.borough = new Text(borough);
		this.week = new Text(week);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.week.write(out);
		this.borough.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.week.readFields(in);
		this.borough.readFields(in);
	}

	public Text getBorough() {
		return this.borough;
	}

	public void setBorough(Text borough) {
		this.borough = borough;
	}

	public Text getWeek() {
		return this.week;
	}

	public void setWeek(Text week) {
		this.week = week;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.borough == null) ? 0 : this.borough.hashCode());
		result = prime * result + ((this.week == null) ? 0 : this.week.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof BoroughWeekWritable)) {
			return false;
		}
		BoroughWeekWritable other = (BoroughWeekWritable) obj;
		if (this.borough == null) {
			if (other.week != null) {
				return false;
			}
		}
		else if (!this.borough.equals(other.borough)) {
			return false;
		}
		if (this.week == null) {
			if (other.week != null) {
				return false;
			}
		}
		else if (!this.week.equals(other.week)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return this.borough + "\t" + this.week;
	}

	@Override
	public int compareTo(BoroughWeekWritable o) {
		if (o == null) {
			throw new NullPointerException();
		}
		if (o == this) {
			return 0;
		}
		if (this.borough.compareTo(o.borough) > 0) {
			return 1;
		}
		else if (this.borough.compareTo(o.borough) == 0) {
			return this.week.compareTo(o.week);
		}
		else {
			return -1;
		}
	}
}
