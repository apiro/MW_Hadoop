import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class FirstMapClass extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
	private int counter;
	private static boolean firstLine = true;
	@Override
	public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
		// I have to skip the first line of the file
		if (!firstLine){
			try {
				String line = value.toString();
				String weekYear = "";
				counter = 0;
				
				DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
				Date date;
				date = format.parse(key.toString());
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int week = c.get(Calendar.WEEK_OF_YEAR);
				String weekStr = week < 10 ? "0" + String.valueOf(week) : String.valueOf(week) ;
				weekYear = String.valueOf(c.get(Calendar.YEAR)) + weekStr;
			
				// Non usare StrinkTokenizer, dato che salta i tokens vuoti
				String[]tokens = line.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				counter += Integer.parseInt(tokens[10]==""?"0":tokens[10]);
				counter += Integer.parseInt(tokens[12]==""?"0":tokens[12]);
				counter += Integer.parseInt(tokens[14]==""?"0":tokens[14]);
				counter += Integer.parseInt(tokens[16]==""?"0":tokens[16]);
				
				output.collect(new Text(weekYear), new IntWritable(counter));
			} catch (Exception e) {
				System.out.println(value.toString());
				e.printStackTrace();
			}
			
		}else{
			firstLine = false;
		}
	}
}
