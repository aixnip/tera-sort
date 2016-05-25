import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.commons.logging.*;

public class HSort extends Configured implements Tool {
	
	private static final Log logger = LogFactory.getLog(HSort.class);

	public static class SMapper extends Mapper<Object, Text, Text, Text> {

			
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			 Text tail = new Text();
			 Text keyOut = new Text();
			 String input = value.toString();  

		     keyOut.set(input.substring(0,10));
			 tail.set(input);
		     
		     context.write(keyOut,tail);

			
		}
	}
	
	static class SReducer extends Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text inputKey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					NullWritable nw = NullWritable.get();
					for(Text val : values) {
					            context.write(nw, val);
					}

		}

	}
	
	public static class SPartitioner extends Partitioner<Text, Text> {
 
	        @Override
	        public int getPartition(Text key, Text value, int numReduceTasks) {
 			   	char ab = key.toString().charAt(0);
	            int ascii = (int)ab;
				int digit = ascii - 32;
           
	           
	            if(numReduceTasks == 0){
	                return 0;
 				}else{
 					return (numReduceTasks*digit/96);
 				}
	        }
	    }

	//the method to call the functions that run the jobs
	public int run(String[] args) throws Exception {
		int reducers = 4;
		try{
			reducers = Integer.parseInt(args[2]);
		}catch(NumberFormatException nfe){
			nfe.printStackTrace();
		}
		formHSortJob(args[0], args[1], reducers);
		
		return 0;

	}

	//method to run the job that forms the MST
	private void formHSortJob(String inputPath, String outputPath, int numReducer)
			throws Exception {
				
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Sorting");
		
		job.setJarByClass(HSort.class);
		job.setMapperClass(SMapper.class);
		job.setReducerClass(SReducer.class);
		job.setPartitionerClass(SPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
 	    job.setNumReduceTasks(numReducer);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files 
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output 
 
		        try {
		            job.waitForCompletion(true);
		        } catch (InterruptedException ex) {
		            logger.error(ex);
		        } catch (ClassNotFoundException ex) {
		            logger.error(ex);
		        }

	}
	
	//main program
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new HSort(), args);
		if (args.length != 2) {
			System.err
					.println("Usage: HSort <in> <output > ");
			System.exit(2);
		}
		System.exit(res);
	}

}
