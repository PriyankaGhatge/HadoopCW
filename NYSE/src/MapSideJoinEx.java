import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapSideJoinEx {
	
	public static class Mapperex extends Mapper<LongWritable, Text, Text,Text>{
		
		private Map<String, String> abmap = new HashMap<String, String>();
		private Map<String, String> abmap1 = new HashMap<String, String>();
		private Text outputkey = new Text();
		private Text outputvalue = new Text();
		
		protected void setup(Context con) throws IOException, InterruptedException{
			
			super.setup(con);
			
			URI[] files = con.getCacheFiles();
			
			Path p =new Path(files[0]);
			
			Path p1 =new Path(files[1]);
			
			if (p.getName().equals("salary.txt")){
				BufferedReader rdr = new BufferedReader(new FileReader(p.toString()));
				String line = rdr.readLine();
				while(line != null){
					String tokens[]= line.split(",");
					String emp_id = tokens[0];
					String emp_sal = tokens[1];
					abmap.put(emp_id, emp_sal);
					line=rdr.readLine();
				}
				rdr.close();
			}
			if (p1.getName().equals("desig.txt")){
				BufferedReader rdr = new BufferedReader(new FileReader(p.toString()));
				String line = rdr.readLine();
				while(line != null){
					String tokens[]= line.split(",");
					String emp_id = tokens[0];
					String emp_desig = tokens[1];
					abmap1.put(emp_id, emp_desig);
					line=rdr.readLine();
				}
				rdr.close();
			}
			
			if(abmap.isEmpty()){
				throw new IOException("Unable to load Salary data");
				}
			if (abmap1.isEmpty()){
				throw new IOException("Unable to load Designation data");
			}
			
		}
		
		protected void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException{
			String row = value.toString();
			String tokens[]= row.split(",");
			String emp_id =tokens[0];
			String salary =abmap.get(emp_id);
			String desig =abmap1.get(emp_id);
			String sal_desig=salary+","+desig;
			outputkey.set(row);
			outputvalue.set(sal_desig);
			con.write(outputkey, outputvalue);
		}
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		 Configuration conf = new Configuration();
		 conf.set("mapreduce.output.textoutputformat.separator", ",");
		 Job job = Job.getInstance(conf);
		 job.setJarByClass(MapSideJoinEx.class);
		 job.setJobName("MapSideJoin");
		 job.setMapperClass(Mapperex.class);
		 job.addCacheFile(new Path("salary.txt").toUri());
		 job.addCacheFile(new Path("desig.txt").toUri());
		 job.setNumReduceTasks(0);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.waitForCompletion(true);
		
	}

}
