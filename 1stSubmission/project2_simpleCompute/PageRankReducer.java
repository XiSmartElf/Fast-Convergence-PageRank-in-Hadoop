import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;
import java.util.LinkedList;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text>
{

	protected void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException 
	{
		Configuration conf=context.getConfiguration();
		float nodeNum = Float.parseFloat(conf.get("nodeNum"));
		float prec = Float.parseFloat(conf.get("precision"));
		float rank = 0.15f / nodeNum;
		float oldRank = 0.0f;
	    String[] str;
	    Text outLinks = new Text();
	    for (Text t : values) {
	      str = t.toString().split(";");
	      if (str.length == 3) {
	        rank += Float.parseFloat(str[1]) / Integer.parseInt(str[2]) * 0.85f;
	      } else {
	        outLinks.set(str[0]);
	        oldRank = Float.parseFloat(str[1]);
	      }
	    }
	    float residuals = Math.abs(oldRank - rank) / rank;
	    context.getCounter(MyCounter.GLOBAL.TOTAL_RESIDUALS).increment((long)(residuals*prec));
	    String outLinksStr = String.valueOf(outLinks.toString());
	    if(outLinksStr.equals("null")){
	    	outLinksStr = "";
	    }
	    context.write(new Text(key.toString() + "," + rank), new Text(outLinksStr));
 	}
}
