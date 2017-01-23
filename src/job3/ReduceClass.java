package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceClass extends MapReduceBase implements Reducer<BoroughWeekWritable, IntWritable, BoroughWeekWritable, Text> {
	long mapperCounter;
	
	@Override
	public void reduce(BoroughWeekWritable key, Iterator<IntWritable> values, OutputCollector<BoroughWeekWritable, Text> output, Reporter reporter)throws IOException {
		Text out = new Text();
		
		double totalLethalAccidents = 0;
		int numberAccidents = 0;

		
		while (values.hasNext()) {
			totalLethalAccidents += values.next().get();
			numberAccidents ++;
        }
		out.set(String.valueOf(numberAccidents) + "\t" + String.valueOf(totalLethalAccidents / numberAccidents));
		output.collect(key, out);
	}
}
