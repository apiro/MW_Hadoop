package job1;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
	@Override
	public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
		try {
			
			String line = value.toString();
			String weekYear = "";
			
			// Extraction of the week and year information from the date of the incident
			DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
			Date date = format.parse(key.toString());
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			int weekNumber = c.get(Calendar.WEEK_OF_YEAR);
			String weekStr = weekNumber < 10 ? "0" + String.valueOf(weekNumber) : String.valueOf(weekNumber) ;
			weekYear = String.valueOf(c.getWeekYear()) + "-" + weekStr;

			// Non usare StrinkTokenizer, dato che salta i tokens vuoti
			String[]tokens = line.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			
			try { 
				if(Integer.parseInt(tokens[10]) > 0) {
					output.collect(new Text(weekYear), new IntWritable(1));
				}
				
			}catch (NumberFormatException e) {
				e.printStackTrace();
			}
			
		} catch (Exception e) {
			System.out.println(value.toString());
			e.printStackTrace();
		}
	}
}
