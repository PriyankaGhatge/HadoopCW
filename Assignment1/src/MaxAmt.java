import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class MaxAmt {

	public static class MapperEx extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable k,Text v, Context con) throws IOException, InterruptedException{

			String Line =v.toString();
			String[] arr =Line.split(";");
			String Cust_id= arr[1];
			int cost = Integer.parseInt(arr[7]);
			con.write(new Text(Cust_id),new LongWritable(cost));

		}
	}

	public static class ReducerEx extends Reducer<Text,LongWritable,Text,LongWritable>{
		long max_amt =0;
		String Cust="";
		public void reduce(Text key,Iterable<LongWritable> values, Context con) 
				throws IOException, InterruptedException{
			long sum=0;

			for(LongWritable val:values)
			{
				sum+=val.get();

				if (max_amt < sum){
					max_amt=sum;
					
					Cust=key.toString();
				}
			
		}
		}

		public void cleanup(Context con) throws IOException, InterruptedException{
			
			con.write(new Text(Cust),new LongWritable(max_amt));
		}

	}



public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Top Customer");
	job.setJarByClass(MaxAmt.class);
	job.setMapperClass(MapperEx.class);
	job.setReducerClass(ReducerEx.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);

}

}



		
