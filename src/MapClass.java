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
	private static boolean firstLine = true;
	@Override
	public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
		// I have to skip the first line of the file
		if (!firstLine){
			try {
				String line = value.toString();
				String weekYear = "";
				int deathsCounter = 0;
				
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
				
				// This is the number of people killed
				deathsCounter = Integer.parseInt(tokens[10]==""?"0":tokens[10]);
				
				// These are the number of people killed per cathegory -> Pedestrian, cyclist, motocyclist 
				// So i don't need them for the calculations
				// deathsCounter += Integer.parseInt(tokens[12]==""?"0":tokens[12]);
				// deathsCounter += Integer.parseInt(tokens[14]==""?"0":tokens[14]);
				// deathsCounter += Integer.parseInt(tokens[16]==""?"0":tokens[16]);
				
				output.collect(new Text(weekYear), new IntWritable(deathsCounter));
			} catch (Exception e) {
				System.out.println(value.toString());
				e.printStackTrace();
			}
			
		}else{
			firstLine = false;
		}
	}
}
