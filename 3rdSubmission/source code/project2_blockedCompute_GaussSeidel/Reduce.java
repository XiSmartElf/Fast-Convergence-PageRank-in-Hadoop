package project2_blockedCompute_GaussSeidel;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable blockId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//define the node info string and new page rank value
		HashMap<Long, Double> degree_map = new HashMap<Long, Double>();
		HashMap<Long, Double> pageRank_map = new HashMap<Long, Double>();
		HashMap<Long, ArrayList<Double>> boundary_map = new HashMap<Long, ArrayList<Double>>();
		HashMap<Long, ArrayList<Long>> edge_map = new HashMap<Long, ArrayList<Long>>();
		HashMap<Long, String> forNextPass = new HashMap<Long, String>();
		HashMap<Long, Double> startMap = new HashMap<Long, Double>();

		//for each item in the list
		for (Text val : values) {

			StringTokenizer itr = new StringTokenizer(val.toString());
			String flag = itr.nextToken();
			
			//************* SET UP ************************
			if (flag.equals("NodeInfo")) {
				long nodeId = Long.parseLong(itr.nextToken());
				double pageRank = Double.parseDouble(itr.nextToken()); 
				pageRank_map.put(nodeId, pageRank);
				forNextPass.put(nodeId, val.toString());
			}
			  //<BE, source node u id, destination node v id, source node degree>, u and v in same block
			else if(flag.equals("BE")){
				long sourceID = Long.parseLong(itr.nextToken());
				long destID = Long.parseLong(itr.nextToken());
				double degree = Double.parseDouble(itr.nextToken()); 
				degree_map.put(sourceID, degree);
				
				//putting edges in this block in the map<destination node ID, sets of incoming sources IDs>
				if(edge_map.containsKey(destID))
					edge_map.get(destID).add(sourceID);
				else{
					ArrayList<Long> sourceSets = new ArrayList<Long>();
					sourceSets.add(sourceID);
					edge_map.put(destID, sourceSets);
				}

			}
			  //<BC, destination node v id, pageRank shared with it> v is in this block
			else if(flag.equals("BC")){
				long destID = Long.parseLong(itr.nextToken());
				double pRShared_ReadOnly = Double.parseDouble(itr.nextToken()); 
				if(boundary_map.containsKey(destID))
					boundary_map.get(destID).add(pRShared_ReadOnly);
				else{
					ArrayList<Double> incomPRshared = new ArrayList<Double>();
					incomPRshared.add(pRShared_ReadOnly);
					boundary_map.put(destID, incomPRshared);
				}
			}
		}
		//clone the page rank map for later residual use
		for(long node:pageRank_map.keySet())
			startMap.put(node, pageRank_map.get(node));

		//*********************** RUNNING ************************

		//for each iteration. loop till it converges
		int counter = 0;
		for(int i=0; i<20; i++)
		{
			double residualSum = 0.0;

			for(long nodeID:pageRank_map.keySet())
			{
				double gain =0.0;
				//get the BE portion of PR
				if(edge_map.containsKey(nodeID)){
					ArrayList<Long>incomEdges = edge_map.get(nodeID);
					//collecting all incoming shared PR
					for(long source:incomEdges)
						gain += pageRank_map.get(source)/degree_map.get(source);
				}

				//get the BC portion of PR 
				if(boundary_map.containsKey(nodeID))
				{
					ArrayList<Double>incomSharedPR = boundary_map.get(nodeID);
					//collecting all incoming shared PR from different blocks
					for(double sharedPR:incomSharedPR)
						gain += sharedPR;
				}
				double oldRank = pageRank_map.get(nodeID);
				double newRank = gain * 0.85 + 0.15/GlobalVals.NUM_OF_NODES;
				pageRank_map.put(nodeID,newRank);
				residualSum += Math.abs((oldRank - newRank)/newRank);
			}
			
			double avgResidual = residualSum/pageRank_map.size();
			counter++;
			if(avgResidual<0.001)
				break;
		}
		//PrintWriter writer = new PrintWriter(new FileWriter("/Users/BboyKellen/Documents/workspace/project2/output/result.txt", true));
		//writer.println("<Reducer> Block: "+blockId+ "; looped for"+counter+" times!");
		//writer.flush();
		//writer.close();
		context.getCounter(Counter.LOOPS).increment((long)(counter));
	
		//calculate the reduce phase residual |(PRstart(v) - PRend(v))| / PRend(v)
		double phaseResidualSum = 0.0;
		for(long node:pageRank_map.keySet())
		{
			phaseResidualSum += Math.abs((startMap.get(node) - pageRank_map.get(node))/pageRank_map.get(node));
			//write the new lines to the output for input for next map-reduce pass
			StringTokenizer itr2 = new StringTokenizer(forNextPass.get(node));
			ArrayList<String> nodeInfo = new ArrayList<String>();
			while(itr2.hasMoreTokens())
				nodeInfo.add(itr2.nextToken());
			//update the page rank to latest
			nodeInfo.set(2, Double.toString(pageRank_map.get(node)));

			StringBuilder sb = new StringBuilder();
			for(int i=2; i<nodeInfo.size();i++)
				sb.append(" "+nodeInfo.get(i));

			//build the new node info string and write to file
			Text updatedNode = new Text(sb.toString().trim());
			context.write(new LongWritable(node), updatedNode);
		}
		//increment the residual counter
		context.getCounter(Counter.RESIDUAL).increment((long)(phaseResidualSum * GlobalVals.CONVER_VAL));

	}

}


