import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Top4_10Product{
	
	public static class MapperEx extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable k,Text v, Context con) throws IOException, InterruptedException{
			
			String Line =v.toString();
			String[] arr =Line.split(";");
			String Prod_id= arr[5];
			int Sales = Integer.parseInt(arr[8]);
			con.write(new Text(Prod_id),new LongWritable(Sales));
		}
	}

	public static class ReducerEx extends Reducer<Text,LongWritable,Text,LongWritable>{
		private TreeMap<Long, String> rep = new TreeMap<Long,String>();
		
		
		public void reduce(Text key,Iterable<LongWritable> values, Context con) 
				throws IOException, InterruptedException{
			long sum=0;
			String Product="";
			 for(LongWritable val:values){
				 sum+=val.get();
				 }
			 Product=key.toString();
			  rep.put(sum,Product);
			  
			  if(rep.size()>4){
				  rep.remove(rep.firstKey());
			  }
		}
		public void cleanup(Context con) throws IOException, InterruptedException{
		
			for(Long t: rep.descendingMap().keySet()){
				con.write(new Text(rep.get(t)),new LongWritable(t));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 4 Products");
		job.setJarByClass(Top4_10Product.class);
		job.setMapperClass(MapperEx.class);
		job.setReducerClass(ReducerEx.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}


