package job2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	long mapperCounter;
	
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
		Text out = new Text();
		
		double totalDeaths = 0;
		int numberAccidents = 0;
		int contributionsCounter = 0;
		
		while (values.hasNext()) {
			String[] vals = values.next().toString().split(",");
			numberAccidents += Integer.parseInt(vals[1]);
			totalDeaths += Integer.parseInt(vals[0]);
			contributionsCounter ++;
        }
		out.set(String.valueOf(numberAccidents) + "\t" + String.valueOf(totalDeaths / contributionsCounter));
		output.collect(key, out);
	}
}
