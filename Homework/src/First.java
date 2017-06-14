/*To find Avg salary given in each city*/

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class First{
	
	public static class MyMap extends Mapper<LongWritable,Text,Text,IntWritable>
	 {
	  public void map(LongWritable k,Text v, Context con)
	  throws IOException, InterruptedException
	  {
	   String line = v.toString();
	   String[] w=line.split("-");
	   String city=w[2];
	   int sal=Integer.parseInt(w[4]);
	   con.write(new Text(city), new IntWritable(sal));
	  }
	 }
	 public static class MyRed extends Reducer<Text,IntWritable,Text,FloatWritable>
	 {
	  public void reduce(Text k, Iterable<IntWritable> vlist, Context con)
	  throws IOException , InterruptedException
	  {
	   int tot =0;
	   int cnt=0;
	   for(IntWritable v:vlist)
	   {
	    tot+=v.get();
	    cnt++;
	   }
	   float avg = tot/cnt;
	   con.write(k,new FloatWritable(avg));
	  }
	 }
	 public static void main(String[] args) throws Exception
	 {
	  Configuration c = new Configuration();
	  Job j= new Job(c,"Average salary");
	  j.setJarByClass(First.class);
	  j.setMapperClass(MyMap.class);
	  j.setReducerClass(MyRed.class);
	  j.setOutputKeyClass(Text.class);
	  j.setOutputValueClass(IntWritable.class);
	  Path p1 = new Path(args[0]);
	  Path p2 = new Path(args[1]);
	    FileInputFormat.addInputPath(j,p1);
	    FileOutputFormat.setOutputPath(j,p2);
	    System.exit(j.waitForCompletion(true) ? 0:1);
	 }

	}

	
