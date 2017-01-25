package job2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
		Text out = new Text();
		
		double totalDeaths = 0;
		int numberAccidents = 0;
		
		while (values.hasNext()) {
			numberAccidents ++;
			totalDeaths += values.next().get();
        }
		out.set(String.valueOf(numberAccidents) + "\t" + String.valueOf(totalDeaths / numberAccidents));
		output.collect(key, out);
	}
}
