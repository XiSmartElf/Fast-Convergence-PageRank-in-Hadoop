package project2_simpleCompute;

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
		  
		  
		  //iterate to get all the outgoing bounds
		  ArrayList<Long> outLinks = new ArrayList<Long>();
	      while (itr.hasMoreTokens()) {
	    	  outLinks.add(Long.parseLong(itr.nextToken()));
	      }
	      
		  Text toShare = new Text();
		  toShare.set("PRval "+nodeId+" "+Double.toString(pageRank / outLinks.size()));
		  
		  //send the node info of this node
		  context.write(new LongWritable(nodeId), new Text("NodeInfo "+ nodeInfo.toString()));
		  
		  //share the RP to each outlink
		  for(long id: outLinks)
			  context.write(new LongWritable(id), toShare);
	}

}
