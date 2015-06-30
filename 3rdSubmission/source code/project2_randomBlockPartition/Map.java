package project2_randomBlockPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {


	public void map(LongWritable key, Text nodeInfo, Context context) throws IOException, InterruptedException {

		  StringTokenizer itr = new StringTokenizer(nodeInfo.toString());
		  
		  long nodeId = Long.parseLong(itr.nextToken());
		  double pageRank = Double.parseDouble(itr.nextToken());
		  long blockId = getBlockID_ofNode(nodeId);
		  
		  //iterate to get all the outgoing bounds
		  ArrayList<Long> outLinks = new ArrayList<Long>();
	      while (itr.hasMoreTokens()) {
	    	  outLinks.add(Long.parseLong(itr.nextToken()));
	      }
	      
	      //parse the line of this Node and write to reducer with its block number for future passes use  
	      //pass <NodeInfo, infoString(nodeId, pageRank, outLink1, outLink2, .... )>
		  context.write(new LongWritable(blockId), new Text("NodeInfo "+ nodeInfo.toString()));		  
		  
		  //to find the inner edges and boundary
		  for(long outLink: outLinks)
		  {
			  long blockId_link = getBlockID_ofNode(outLink);
			  //parse BE, the edges from u->v  this node is u and u & v are in the same block
			  if(blockId_link==blockId)
			  {
				  //pass <BE, source node id, destination node id, source node degree>
				  Text BE = new Text();
				  BE.set("BE "+nodeId+" "+outLink+" "+outLinks.size());
				  context.write(new LongWritable(blockId_link), BE);
			  }
			  //parse BC, the edges from u->v  this node is u and u & v are NOT in the same block
			  else
			  {
				  Text BC = new Text();
				  //pass <BC, destination node id, pageRank shared with it>.
				  BC.set("BC "+outLink+" "+Double.toString(pageRank / outLinks.size()));
				  context.write(new LongWritable(blockId_link), BC);
			  }
		  }
		  
	}
	
	
	
	
	/**
	 * @return take the nodeID and return its block ID
	 */
	//block.txt: number of Nodes included in the corresponding Block. 68 lines. total 685230 nodes
	static long getBlockID_ofNode(Long nodeID)
	{	
		return nodeID.hashCode() % GlobalVals.NUM_OF_BLOCKS;		
	}

}
