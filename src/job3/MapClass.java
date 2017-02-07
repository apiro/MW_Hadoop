package job3;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, BoroughWeekWritable, IntWritable> {
	private static IntWritable noDeaths = new IntWritable(0); 
	private static IntWritable deaths = new IntWritable(1); 
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<BoroughWeekWritable, IntWritable> output, Reporter reporter) throws IOException {
		try {
			String line = value.toString();
			// Do not use StringTokenizer, since it skips empty records
			String[] tokens = line.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			
			// Create the year-week key from the date
			DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
			Date date = format.parse(tokens[0]);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			int weekNumber = c.get(Calendar.WEEK_OF_YEAR);
			String weekStr = weekNumber < 10 ? "0" + String.valueOf(weekNumber) : String.valueOf(weekNumber) ;
			String weekYear = String.valueOf(c.getWeekYear()) + "-" + weekStr;
			
			// If the accidents is lethal (number of deaths > 0)
			if (Integer.parseInt(tokens[11]) > 0 ){
				output.collect(new BoroughWeekWritable(tokens[2], weekYear), deaths);
			}else{
				output.collect(new BoroughWeekWritable(tokens[2], weekYear), noDeaths);
			}
		} catch (Exception e) {
			System.out.println(value.toString());
			e.printStackTrace();
		}
	}
}
