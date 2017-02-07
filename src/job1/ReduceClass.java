package job1;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)throws IOException {
		int deaths = 0;
		
		// For each value in the group
		while (values.hasNext()) {
			// Count the number of deaths
            deaths += Integer.parseInt(values.next().toString());
        }
		
		// Write output to file
		output.collect(key, new IntWritable(deaths));
	}

}
