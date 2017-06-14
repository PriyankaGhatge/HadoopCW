/*To find highest paid employee name and salary iin each department of a city*/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Fourth {

	public static class MapperEx extends Mapper<Object, Text, Text, Text> {

		public void map(Object key,Text value,Context ctx) throws IOException, InterruptedException
		{
		String[] arr=value.toString().split("-");
		
		String Emp_name = arr[1];
		int sal = Integer.parseInt(arr[4]);
		String dept_city = arr[3]+"-"+arr[2];;
		ctx.write(new Text(dept_city), new Text(Emp_name+ "-" +sal));
		}
		}
	
	public static class ReducerEx extends Reducer<Text, Text,Text, Text>
	{
	public void reduce(Text key,Iterable<Text> itr,Context context) throws IOException, InterruptedException
	{ 
	int maxsal=0;
	
	String sal ="";
	String Dept_city="";

	for (Text val : itr){
	String arr[] = val.toString().split("-");
	if (maxsal < Integer.parseInt(arr[1]))
	{
	maxsal = Integer.parseInt(arr[1]);
	sal = arr[1].toString();

	Dept_city= arr[0].toString();
	
	}

	}
	context.write(new Text(key), new Text(Dept_city.toString()+"-" + sal.toString()));

	}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Fourth.class);
		job.setMapperClass(MapperEx.class);
		job.setReducerClass(ReducerEx.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

}