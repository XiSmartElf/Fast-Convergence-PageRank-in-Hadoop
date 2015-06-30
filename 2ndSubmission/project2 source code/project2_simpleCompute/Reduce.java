package project2_simpleCompute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable nodeID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//define the node info string and new page rank value
		Double newPR_val = 0.0;
		Text thisNodeInfo = new Text();
		
		//for each item in the list
		for (Text val : values) {
			
			StringTokenizer itr = new StringTokenizer(val.toString());
			String flag = itr.nextToken();
			
			//if the item is node info of this node //if the item is the page rank shared to this node from other node;
			if (flag.equals("NodeInfo")) {
				thisNodeInfo.set(val.toString());
			}
			else if(flag.equals("PRval")){
				itr.nextToken();//erase the id that shares the PR to you
				newPR_val += Double.parseDouble(itr.nextToken());
			}
		}
		newPR_val = newPR_val * 0.85 + 0.15 / GlobalVals.NUM_OF_NODES;
		
		StringTokenizer itr = new StringTokenizer(thisNodeInfo.toString());
		ArrayList<String> nodeInfo = new ArrayList<String>();
		while(itr.hasMoreTokens())
			nodeInfo.add(itr.nextToken());
		
		//update the node info with the new page rank in the third field
		Double oldVal = Double.parseDouble(nodeInfo.get(2));
		nodeInfo.set(2, Double.toString(newPR_val));
		StringBuilder sb = new StringBuilder();
		for(int i=2; i<nodeInfo.size();i++)
			sb.append(" "+nodeInfo.get(i));
		
		//build the new node info string and write to file
		Text updatedNode = new Text(sb.toString().trim());
		context.write(nodeID, updatedNode);
		
		//calculate residual and increment the counter
		Double residual = Math.abs((oldVal-newPR_val)/newPR_val);
		context.getCounter(Counter.RESIDUAL).increment((long)(residual * GlobalVals.CONVER_VAL));
		
		
	}

}
