import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class StringSearchEx {
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text sentence  = new Text();
		
		public void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException{
			
			String searchText = con.getConfiguration().get("myText");
			String line = value.toString();
			String newline = line.toLowerCase();
			String newText = searchText.toLowerCase();
			
			if(searchText != null){
				if(newline.contains(newText))
				{
					sentence.set(newline);
					con.write(sentence,one);
				}
			}
			
		}
	}
	 public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		  
		 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			 int sum = 0;
			 for (IntWritable val : values) {
				 sum += val.get();
				 
			 }
			 result.set(sum);
			 context.write(key, result);
		 } 
			 
		 }
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		 if(args.length > 2){
			 conf.set("myText", args[2]);
		 }
		    
		    Job job = Job.getInstance(conf, "word Count");
		    job.setJarByClass(StringSearchEx.class);
		    job.setMapperClass(TokenizerMapper.class);
		    //job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    //job.setNumReduceTasks(0);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
