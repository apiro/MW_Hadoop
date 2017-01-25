package job2;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		ArrayList<String> tmp_factors = new ArrayList<String>();
		
		String line = value.toString();
		// Do not use StringTokenizer, since it skips empty records
		String[]tokens = line.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		
		// Loop through all the columns of the contribution factors 
		for (int i = 18; i <= 22 ; i ++){
			if (!tokens[i].isEmpty()){ // if contribution factor is not empty
				
				// If the contribution factor is the same of one of this accident,
				// then don't count it as an other accident, otherwise
				// keep track of the accident -> toCount = 1				
				if (!tmp_factors.contains(tokens[i])){
					tmp_factors.add(tokens[i]);
					output.collect(new Text(tokens[i]), new IntWritable(Integer.parseInt(tokens[11])));
				}
			}
		}
	}
}
