package job2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
	long mapperCounter;
	
	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
		Text out = new Text();
		
		int totalDeaths = 0;
		int numberAccidents = 0;
		
		while (values.hasNext()) {
			System.out.println(key + "---" + values.next());
			numberAccidents ++;
			//totalDeaths += Integer.parseInt(values.next().toString());
        }
		//System.out.println(totalDeaths + " -- " + sameContributionsNumber + " -- " + numberAccidents);
		out.set(String.valueOf(numberAccidents));
		out.set(String.valueOf(totalDeaths));
		output.collect(key, out);
	}
}
