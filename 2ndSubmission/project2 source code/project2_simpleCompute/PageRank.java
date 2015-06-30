package project2_simpleCompute;

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
		double avgResidual =1;
		//PrintWriter writer = new PrintWriter(new FileWriter("/Users/BboyKellen/Documents/workspace/project2/output/result.txt", true));

		while(count<5)
		{
			Job job = Job.getInstance(conf, "pageRank");
			
			job.setJarByClass(project2_simpleCompute.PageRank.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
	
			// specify output types
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
	
			// specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job, new Path(args[0]+count));
			FileOutputFormat.setOutputPath(job, new Path(args[0]+(count+1)));
	
		    job.waitForCompletion(true);
		    count++;
			avgResidual = 1.0 * job.getCounters().findCounter(Counter.RESIDUAL).getValue() / GlobalVals.CONVER_VAL / GlobalVals.NUM_OF_NODES;
			System.out.println("The avergae residual of iter"+(count-1)+" is "+ avgResidual);
			//writer.println("The avergae residual of iter"+(count-1)+" is "+ avgResidual);	
			//writer.flush();

		}
		//writer.close();
		System.exit(0);
	}

}
