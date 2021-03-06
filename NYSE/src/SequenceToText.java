import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class SequenceToText {
	
	public static class SequenceMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			con.write(key,value);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "Convert text to sequence");
	    job.setJarByClass(SequenceToText.class);
	    job.setMapperClass(SequenceMapper.class);
	    //job.setCombinerClass(ReduceClass.class);
	    //job.setReducerClass(ReduceClass.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}
