package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceClass extends MapReduceBase implements Reducer<BoroughWeekWritable, IntWritable, Text, Text> {
	long mapperCounter;
	
	@Override
	public void reduce(BoroughWeekWritable key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
		Text out = new Text();
		Text keyOut = new Text();
		
		double totalLethalAccidents = 0;
		int totalAccidents = 0;
		
		while (values.hasNext()) {
			totalLethalAccidents += values.next().get();
			totalAccidents ++;
        }
		out.set(String.valueOf(totalAccidents) + "," + String.valueOf(totalLethalAccidents/totalAccidents));
		keyOut.set(key.getBorough().toString() + " - " + key.getWeek().toString());

		output.collect(keyOut, out);
	}
}
