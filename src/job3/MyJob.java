package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Average number of accidents and average number of lethal accidents per week per borough
 * We need to use multiple MapReduce chained together
 * 
 * 1. 
 *  Map -> (TextInputFormat)
 *  	Extract for each (Borough-Week) key :
 * 			- 1 if there is a lethal accident
 *  		- 0 otherwise
 *  	Output:
 *  		- Key: Borough-Week
 *  		- 1/0
 *  Reduce -> 
 *  	For each Borough-Week key:
 *  		- Keep counter of number of accidents (cardinality of Iterator<IntWritable> values)
 *  		- Keep counter of number of lethal accidents (sum of values elements)
 *  	Output:
 *  		- Key: Borough
 *  		- Values: totalLethalAccidents, TotalAccidents
 *  
 *  Temp output File has these columns comma separated
 *  Borough, Week, totalLethalAccidents, TotalAccidents
 * 
 * 2.
 * 	Map -> (KeyValueTextInputFormat)
 * 		Key is the borough
 * 		output (Key, Value)
 * 	Reduce ->
 * 		For each key (borough):
 * 			For each element in value iterator
 * 				sum all the accidents
 * 				sum all the lethalAccidents
 * 				count all the entries (number of weeks)
 *		Output:
 *			- key: borough
 *			- values: accidents/totalWeeks, lethalAccidents/totalWeeks
 */
public class MyJob extends Configured implements Tool{
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
        JobConf jobConf1 = new JobConf(conf, MyJob.class);

        Path in = new Path(arg0[0]);
        Path out = new Path(arg0[1]);
        FileInputFormat.setInputPaths(jobConf1, in);
        FileOutputFormat.setOutputPath(jobConf1, out);

        jobConf1.setJobName("Job");
        jobConf1.setMapperClass(MapClass.class);
        jobConf1.setReducerClass(ReduceClass.class);
        jobConf1.setInputFormat(TextInputFormat.class);
        jobConf1.set("key.value.separator.in.input.line", ","); 
        jobConf1.set("mapreduce.output.textoutputformat.separator", ",");
        jobConf1.setOutputFormat(TextOutputFormat.class);
        jobConf1.setOutputKeyClass(BoroughWeekWritable.class);
        jobConf1.setOutputValueClass(IntWritable.class);
        
        JobClient.runJob(jobConf1);
        
        /*JobConf jobConf2 = new JobConf(conf, MyJob.class);
        
        Path in2 = new Path(intermediate);
	    Path out2 = new Path(arg0[1]);
	    FileInputFormat.setInputPaths(jobConf2, in2);
	    FileOutputFormat.setOutputPath(jobConf2, out2);
	    
	    jobConf2.setJobName("Job3 Final");
	    jobConf2.setMapperClass(SecondMapClass.class);
	    jobConf2.setReducerClass(SecondReduceClass.class);
	    
	    jobConf2.setInputFormat(KeyValueTextInputFormat.class);
	    jobConf2.set("key.value.separator.in.input.line", ","); 
	    jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
	    jobConf2.setOutputFormat(TextOutputFormat.class); 
	    jobConf2.setOutputKeyClass(Text.class);
	    jobConf2.setOutputValueClass(Text.class);

        Job job1 = new Job(jobConf1);
        Job job2 = new Job(jobConf2);
        JobControl jobControl = new JobControl("Chaining");
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
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}