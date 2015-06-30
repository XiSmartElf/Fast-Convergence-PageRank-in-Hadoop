package project2_blockedCompute;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

public class twoNumber {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		File file = new File("/Users/BboyKellen/Documents/workspace/project2/input/iter6/part-r-00000");
		Scanner inFile = new Scanner(file);

		PrintWriter writer = new PrintWriter(new FileWriter("/Users/BboyKellen/Documents/workspace/project2/output/twoNumbers.txt", true));
		
		long target = 0;
		HashMap<Long,String> set = new HashMap<Long, String>();
		
		for(int i=0; i<GlobalVals.block.length; i++){
			set.put(target,"");
			set.put(target+1,"");
			target += GlobalVals.block[i];
		}
		
		
		while(inFile.hasNextLine())
		{
			String line = inFile.nextLine();
			StringTokenizer tokens = new StringTokenizer(line);
			long nodeID = Long.parseLong(tokens.nextToken());

			if(set.containsKey(nodeID))
				set.put(nodeID, "Block "+Map.getBlockID_ofNode(nodeID) +": nodeID " +nodeID+" PageRank val: "+tokens.nextToken());
		}
		
		
		Object[] sorted = set.keySet().toArray();
		Arrays.sort(sorted);
		for(int i=0; i<sorted.length;i++)
			writer.println(set.get((long)sorted[i]));
		writer.close();
	}

}
