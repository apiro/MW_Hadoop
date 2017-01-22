package job2;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	// List of all the contribution factors
	private static ArrayList<String> factors = new ArrayList<String>();
	
	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		ArrayList<String> tmp_factors = new ArrayList<String>();
		Text out = new Text();

		String line = value.toString();
		// Do not use StringTokenizer, since it skips empty records
		String[]tokens = line.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		
		// Loop through all the columns of the contribution factors 
		for (int i = 17; i <= 21 ; i ++){
			if (!tokens[i].isEmpty()){ // if contribution factor is not empty
				if (!factors.contains(tokens[i])){
					factors.add(tokens[i]);
				}	
				
				String toCount = "0";
				if (!tmp_factors.contains(tokens[i])){
					toCount = "1";
					tmp_factors.add(tokens[i]);
				}
				out.set(tokens[10] + "," + toCount);
				output.collect(new Text(tokens[i]), out);
			}
		}
	}
}
