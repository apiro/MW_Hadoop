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
	
	/*
	 * Brainstorming. Per questo progetto credo servano 2 jobs
	 * Il primo estrae per ogni incidente il numero di morti (data, #morti)
	 * Il secondo raggruppa i dati del job precedente per settimana e somma il numero di morti
	 */
	
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
        
		/* Job 2 */
        /*JobConf jobConf2 = new JobConf(conf, Job1.class);

        Path in2 = new Path(arg0[0]);
        Path out2 = new Path(arg0[1]);
        FileInputFormat.setInputPaths(jobConf2, in2);
        FileOutputFormat.setOutputPath(jobConf2, out2);

        jobConf2.setJobName("Job1");
        jobConf2.setMapperClass(FirstMapClass.class);
        jobConf2.setReducerClass(FirstReduceClass.class);
        jobConf2.setInputFormat(KeyValueTextInputFormat.class);
        jobConf2.set("key.value.separator.in.input.line", ",");  
        jobConf2.setOutputFormat(TextOutputFormat.class);
        jobConf2.setOutputKeyClass(Text.class);
        jobConf2.setOutputValueClass(Text.class);
        
        ControlledJob job1 = new ControlledJob(jobConf1);
        ControlledJob job2 = new ControlledJob(jobConf2);
        
        JobControl jobControl = new JobControl("chaining");
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        job2.addDependingJob(job1);
        
        Thread t = new Thread(jobControl); 
        t.setDaemon(true);
        t.start(); 
                      
        while (!jobControl.allFinished()) { 
          try { 
            Thread.sleep(1000); 
          } catch (InterruptedException e) { 
            // Ignore. 
          } 
        } 
        */
        
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Job1(), args);
        System.exit(res);
    }
	
}
