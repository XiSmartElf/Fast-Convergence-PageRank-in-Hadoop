package project2_blockedCompute;

import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

	public static void main(String[] args) throws Exception {
		org.apache.log4j.BasicConfigurator.configure();

		Configuration conf = new Configuration();

		int count =0;
		double avgResidual = 1;
		PrintWriter writer = new PrintWriter(new FileWriter("/Users/BboyKellen/Documents/workspace/project2/output/result.txt", true));
		while(avgResidual>0.001)
		{
			Job job = Job.getInstance(conf, "pageRank");
			
			job.setJarByClass(project2_simpleCompute.PageRank.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
	
			// specify output types
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
	
			// specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job, new Path("/Users/BboyKellen/Documents/workspace/project2/input/iter"+Integer.toString(count)+"/part-r-00000"));
			FileOutputFormat.setOutputPath(job, new Path("/Users/BboyKellen/Documents/workspace/project2/input/iter"+Integer.toString(count+1)));
	
		    job.waitForCompletion(true);
		    count++;
			avgResidual = 1.0 * job.getCounters().findCounter(Counter.RESIDUAL).getValue() / GlobalVals.CONVER_VAL / GlobalVals.NUM_OF_NODES;
			//System.out.println("The avergae residual of iter"+count+" is "+ avgResidual);
			double avgLoops = 1.0 * job.getCounters().findCounter(Counter.LOOPS).getValue() / GlobalVals.CONVER_VAL / GlobalVals.NUM_OF_BLOCKS;
			writer.println("The avergae residual of iter"+count+" is "+ avgResidual);	
			writer.println("The avergae iterations of pass "+count+" is "+ avgLoops);	
			writer.flush();
		}
		writer.close();
		System.exit(0);
	}

}
