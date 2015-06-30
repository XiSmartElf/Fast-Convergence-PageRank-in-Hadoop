/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.Counter;


public class PageRankMain {
    
    public static String inputDirectory;
    public static String outputDirectory;
    public static String nodeNum;
    public static final String PRECISION = "10000";

    /**
     * Create a map-reduce job for one time
     */
    public static Job createJob(String inputDirectory, String outputDirectory, String jobName, String nodeNum)
        throws IOException
    {
        
        Configuration conf = new Configuration();
        conf.set("nodeNum",nodeNum);
        conf.set("precision",PRECISION);
        
        Job job = new Job(conf, jobName);
        job.setJarByClass(PageRankMain.class);

        KeyValueTextInputFormat.addInputPath(job, new Path(inputDirectory));
        TextOutputFormat.setOutputPath(job, new Path(outputDirectory));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static int manageJobs(int maxIteration, String inputDirectory,
        String outputDirectory, String nodeNum) throws Exception {
    	long totalResiduals = 0l;
    	int i = 0;
    	Job[] jobs = new Job[maxIteration];
    	while(i < maxIteration){
    		if(i==0){
    			jobs[i] = createJob(inputDirectory, outputDirectory+i, "job"+i, nodeNum);
    		} else if(i< maxIteration-1){
    			jobs[i] = createJob(outputDirectory+(i-1), outputDirectory+i, "job"+i, nodeNum);
    		} else {
    			jobs[i] = createJob(outputDirectory+(i-1), outputDirectory, "job"+i, nodeNum);
    		}
    		jobs[i].waitForCompletion(true);
    		totalResiduals = jobs[i].getCounters().findCounter(MyCounter.GLOBAL.TOTAL_RESIDUALS).getValue();
    		//totalResiduals = jobs[i].getCounters().countCounters();
    		System.out.println("----------------------------");
        	System.out.println("Job " + i + " Finished.");
            System.out.println("****************************");
            //System.out.println("Avg residuals is " + totalResiduals / Float.parseFloat(PRECISION) / Float.parseFloat(nodeNum));
            System.out.println("Avg residuals is " + totalResiduals / Float.parseFloat(PRECISION) / Float.parseFloat(nodeNum));
        	System.out.println("Total residuals is " + totalResiduals);
            System.out.println("----------------------------");
        	jobs[i].getCounters().findCounter(MyCounter.GLOBAL.TOTAL_RESIDUALS).setValue(0l);
    		i++;
    	}
    	return i;
    }

    /**
	 *	The main function for our pagerank mapreduce.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // Note that command line arguments are not sanitized
        if (otherArgs.length == 3)
        {
            inputDirectory = otherArgs[0];
            outputDirectory = otherArgs[1];
            nodeNum = otherArgs[2];
        }
        else
        {
            System.err.println("Usage:");
            System.err.println("\t<in directory> <out directory> <number of nodes>");
            System.err.println("Where:");
            System.err.println("\t<in directory> is the input directory containing files of points to cluster");
            System.err.println("\t<out directory> is the output directory to which Hadoop files are written");
            System.err.println("\t<number of nodes> is the number of nodes in graph");
            System.exit(2);
        }

        System.out.println("============================");
        System.out.println("Start of iterations");
        System.out.println("============================");
        // At this point, the centroids have been initialized.  This calls the student implemented
        // Jobs.
        int iters = manageJobs(5, inputDirectory, outputDirectory, nodeNum);

        System.out.println("============================");
        System.out.println("PageRank convergence successful.");
        System.out.println("----------------------------");
        System.out.println("Total Iteration times: "+iters);
        System.out.println("============================");
        System.exit(0);
    }
}
