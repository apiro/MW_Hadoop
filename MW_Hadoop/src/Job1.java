import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * This Job is used to count number of lethal accidents 
 * per week throughout the entire dataset
 * @author Alberto
 */
public class Job1 extends Configured implements Tool{
		
	// Path to temporary folder
	private static String intermediate = "/temp";
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		/* Job 1 */
        JobConf jobConf1 = new JobConf(conf, Job1.class);

        Path in = new Path(arg0[0]);
        Path out = new Path(arg0[1]);
        FileInputFormat.setInputPaths(jobConf1, in);
        FileOutputFormat.setOutputPath(jobConf1, out);

        jobConf1.setJobName("Job1");
        jobConf1.setMapperClass(FirstMapClass.class);
        jobConf1.setReducerClass(FirstReduceClass.class);
        jobConf1.setInputFormat(KeyValueTextInputFormat.class);
        jobConf1.set("key.value.separator.in.input.line", ",");  
        jobConf1.setOutputFormat(TextOutputFormat.class);
        jobConf1.setOutputKeyClass(Text.class);
        jobConf1.setOutputValueClass(IntWritable.class);
        
        JobClient.runJob(jobConf1);

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Job1(), args);
        System.exit(res);
    }
	
}
