/* Total amount a Company spends on salary of Employee*/

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


public class Fifth {
	
	 public static class MapperEx extends Mapper<LongWritable,Text,Text,IntWritable>
	 {
	  public void map(LongWritable k,Text v, Context con)
	  throws IOException, InterruptedException
	  {
	   String line = v.toString();
	   String[] w=line.split("-");
	   int sal=Integer.parseInt(w[4]);
	   con.write(new Text("Highest Salary"), new IntWritable(sal));
	  }
	 }
	 public static class ReducerEx extends Reducer<Text,IntWritable,IntWritable,Text>
	 {
	  public void reduce(Text k, Iterable<IntWritable> vlist, Context con)
	  throws IOException , InterruptedException
	  {
	   int tot =0;
	   for(IntWritable v:vlist)
	    tot+=v.get();
	   con.write(new IntWritable(tot), new Text());
	  }
	 }

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c = new Configuration();
		  Job j= new Job(c,"Test");
		  j.setJarByClass(Fifth.class);
		  j.setMapperClass(MapperEx.class);
		  j.setReducerClass(ReducerEx.class);
		  j.setOutputKeyClass(Text.class);
		  j.setOutputValueClass(IntWritable.class);
		  Path p1 = new Path(args[0]);
		  Path p2 = new Path(args[1]);
		    FileInputFormat.addInputPath(j,p1);
		    FileOutputFormat.setOutputPath(j,p2);
		    System.exit(j.waitForCompletion(true) ? 0:1);
		
	}

}
