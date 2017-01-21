import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text arg0, Text arg1, OutputCollector<Text, Text> arg2,
			Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
