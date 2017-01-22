package job2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Number of accidents and average number of deaths per contributing factor in the dataset
 * Map -> 
 * for each accident, 
 * 		for each contributing factor	
 * 			set # deaths
 * 
 * reduce ->
 * for each record in the map
 * 		output: contributing factor, sum of deaths / # contributing factors, accidents = number of factors /w same value
 * 
 * TODO - How to find #contributing factors?
 */
public class MyJob extends Configured implements Tool{
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		/* Job 1 */
        JobConf jobConf1 = new JobConf(conf, MyJob.class);

        Path in = new Path(arg0[0]);
        Path out = new Path(arg0[1]);
        FileInputFormat.setInputPaths(jobConf1, in);
        FileOutputFormat.setOutputPath(jobConf1, out);

        jobConf1.setJobName("Job1");
        jobConf1.setMapperClass(MapClass.class);
        jobConf1.setReducerClass(ReduceClass.class);
        jobConf1.setInputFormat(KeyValueTextInputFormat.class);
        jobConf1.set("key.value.separator.in.input.line", ",");  
        jobConf1.setOutputFormat(TextOutputFormat.class);
        jobConf1.setOutputKeyClass(Text.class);
        //jobConf1.setOutputValueClass(IntWritable.class);
        jobConf1.setOutputValueClass(Text.class);
        
        JobClient.runJob(jobConf1);

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}