import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;



public class FileConverter {
	private static String inputPath = "edges.txt";
	private static String outputPath = "myedges.txt";
	private static String blockPath = "blocks.txt";
	private List<Long> blocks = new ArrayList<Long>();
	
	public FileConverter(String blockPath){
		try {
			FileReader reader = new FileReader(blockPath);
			BufferedReader br = new BufferedReader(reader);
			String str = null;    
			long sum = 0;
        	while((str = br.readLine()) != null) {
        		str = str.trim();
        		blocks.add(Long.parseLong(str) + sum);
        		sum += Long.parseLong(str);
        	}
        	br.close();
        	reader.close();
        	
		}
		catch(Exception e){
			e.printStackTrace();
		}
    	
	}
	
	public static void processFile(String inputFilePath, String outputFilePath) {
		double fromNetID = 0.342; // 576 is 675 reversed
		double rejectMin = 0.9 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		
        try {
        	StringBuffer sb= new StringBuffer();
        	sb.append('#');
        	
        	FileReader reader = new FileReader(inputFilePath);
        	BufferedReader br = new BufferedReader(reader);
        	
        	FileWriter writer = new FileWriter(outputFilePath);
        	BufferedWriter bw = new BufferedWriter(writer);
        	
        	int prev = -1;       	
        	String str = null;
        	int counter = 0;
        	while((str = br.readLine()) != null) { 
              	String[] strs = str.split("\\s+");       	
              	if(Double.parseDouble(strs[1]) >= rejectMin && 
              	   Double.parseDouble(strs[1]) < rejectLimit){
              		continue;
              	}              	             	
              	int curr = Integer.parseInt(strs[2]);
              	while(counter + 1 < curr){
              		sb.deleteCharAt(sb.length() - 1);
              		counter ++;
              		sb.append("#" + counter + " ");
              		if(counter + 1 == curr){
              			continue;
              		}
              	}	
              	if(curr != prev){
              		sb.deleteCharAt(sb.length() - 1);
              		sb.append('#' + strs[2] + " " + strs[3] + ",");
              		prev = curr;
              		counter = curr;
              	}
              	else{
              		sb.append(strs[3] + ",");  	
              	}
        	}
        	String output = sb.substring(1, sb.length() - 1);
        	String[] outputs = output.split("#");

        	for(String line : outputs){
        		bw.write(line);
        		bw.newLine();
        	}
        	
        	br.close();
        	reader.close();
        
        	bw.close();
        	writer.close();
        }
        catch(Exception e) {
              e.printStackTrace();
        }
	}
	
	
	public long blockIDofNode(long nodeID){
		if(blocks == null || blocks.size() == 0){
			return -1;
		}
		int start = 0;
		int end = blocks.size() - 1;
		int mid = 0;
		
		while(start + 1 < end){
			mid = (start + end) / 2;
			if(blocks.get(mid) > nodeID){
				end = mid;
			}
			else if(blocks.get(mid) < nodeID){
				start = mid;
			}
			else{
				return mid;
			}		
		}
		if(blocks.get(start) >= nodeID){
			return 0;
		}
		
		return end;
	}
		
    public static void main(String[] args) {
    	processFile(inputPath,outputPath);   
    	//FileConverter fc = new FileConverter("//Users//Trees//Desktop//blocks.txt");
    	//System.out.println(fc.blockIDofNode(685321));
    }
    
}
