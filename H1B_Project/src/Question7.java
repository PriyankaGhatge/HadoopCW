import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Question7 {
	
	public static class map_app extends Mapper<LongWritable,Text,Text,Text>
	{
	
	public void map(LongWritable key,Text Value,Context con) throws IOException, InterruptedException
	{
		String[] rec=Value.toString().split("\t");
		String year=rec[7];
		String application=rec[1];
		con.write(new Text(year),new Text(application));
}
	}
	
	public static class red extends Reducer<Text,Text,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
		{
			int count=0;
			for(Text val:value)
			{
				count++;
			}
			con.write(key, new IntWritable(count));
		}
}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		Configuration con=new Configuration();
		Job job=Job.getInstance(con,"Q7");
		job.setJarByClass(Question7.class);
		job.setMapperClass(map_app.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
