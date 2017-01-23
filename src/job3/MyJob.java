package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Average number of accidents and average number of lethal accidents per week per borough
 * Map -> 
 * for each accident, 
 * 		output => Key:brough, value: week, #hasDeaths
 * 
 * Reduce ->
 * for each value /w same key
 * 		Average number of accidents = values.length
 * 		Average number of lethal accidents = values /w hasDeaths == true
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

        jobConf1.setJobName("Job3");
        jobConf1.setMapperClass(MapClass.class);
        jobConf1.setReducerClass(ReduceClass.class);
        jobConf1.setInputFormat(KeyValueTextInputFormat.class);
        jobConf1.set("key.value.separator.in.input.line", ",");  
        jobConf1.setOutputFormat(TextOutputFormat.class);
        jobConf1.setOutputKeyClass(BoroughWeekWritable.class);
        jobConf1.setOutputValueClass(IntWritable.class);
        //jobConf1.setOutputValueClass(Text.class);
        
        JobClient.runJob(jobConf1);

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}