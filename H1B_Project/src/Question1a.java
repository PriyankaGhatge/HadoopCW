import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question1a {
	
	public static class MapperEx extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException{
			String Line = value.toString();
			String[] arr =Line.split("\t");
			
			String Year = arr[7];
			String Job_title = arr[4];
			int count = 0;
			
			if(Job_title.equals("DATA ENGINEER")){
				count++;
				con.write(new Text(Year),new IntWritable(count));
			}
		}
	}
	
	
	public static class ReducerEx extends Reducer<Text,IntWritable,Text,Text>{
		
		int year = 0;		
		String output = " ";
		public  void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int count = 0;
			double growthpercentage= 0; 
			for (IntWritable val:values){
				count += val.get();
			}
			if(year!=0){
				growthpercentage = ((double)(count-year)/year)*100;
				String.format("%.2f%%", growthpercentage);
				
			}
			output = String.format("%d",count)+","+growthpercentage;
			year = count;
			
			context.write(key,new Text(output));
		}
	}		

		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		  
		  Job job = Job.getInstance(conf, "Question1a");
		  job.setJarByClass(Question1a.class);
		  job.setMapperClass(MapperEx.class);
		  job.setReducerClass(ReducerEx.class);
		  job.setOutputKeyClass(Text.class);
		  
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  job.setOutputValueClass(Text.class);
		  
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		
	}

}
