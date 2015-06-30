import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Text>
{
	private static final Log log = LogFactory.getLog(PageRankMapper.class);

  public void map(Text key, Text value, Context context)
         throws IOException, InterruptedException
  {
    String[] outLinks = value.toString().split(",");
    String[] link = key.toString().split(",");
    Configuration conf=context.getConfiguration();
    float nodeNum = Float.parseFloat(conf.get("nodeNum"));
    float rank = 0.15f / nodeNum;
    if (link.length > 1) {
      rank = Float.parseFloat(link[1]);
    }
    int outLinkLen = outLinks.length;
    for (String s : outLinks) {
      if(!s.equals("")){
        context.write(new Text(s), new Text(link[0] + ";" + rank + ";" + outLinkLen));
      }
    }
    value = new Text(value.toString()+";"+rank);
    context.write(new Text(link[0]), value);
  }

}
  
