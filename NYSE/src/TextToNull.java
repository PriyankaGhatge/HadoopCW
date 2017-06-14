import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class TextToNull {

	public static class MapperEx extends Mapper<LongWritable,Text,LongWritable,Text>
	   {
	      public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	      {	 
	    	  String record = value.toString();
	    	  String [] parts =record.split(",");
	    	  int mykey = Integer.parseInt(parts[0]);
	    	  con.write(new LongWritable(mykey), new Text(parts[1]));
	      }
	   }
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Convert text to null");
	    job.setJarByClass(TextToNull.class);
	    job.setMapperClass(MapperEx.class);
	    
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(NullOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
