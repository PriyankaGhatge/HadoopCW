import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KeyValueToTextEx {

	public static class KeyValueMapper extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context con) throws IOException, InterruptedException{
			con.write(key,value);
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","#");
		    Job job = Job.getInstance(conf, "Break string into keyand value");
		    job.setJarByClass(StockVolume.class);
		    job.setMapperClass(KeyValueMapper.class);
		    //job.setCombinerClass(ReduceClass.class);
		    //job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(0);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setInputFormatClass(KeyValueTextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
