package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
		Text out = new Text();
		float totalLethalAccidents = 0;
		float totalAccidents = 0;
		int weekCounter = 0;
		
		while (values.hasNext()) {
			String[] vals = values.next().toString().split(",");
			totalLethalAccidents += Integer.parseInt(vals[0]);
			totalAccidents += Integer.parseInt(vals[1]);
			weekCounter ++;
        }
		out.set(String.valueOf(totalAccidents / weekCounter) + "," + String.valueOf(totalLethalAccidents / weekCounter));
		output.collect(key, out);
	}
}
