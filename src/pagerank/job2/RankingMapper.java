package pagerank.job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankingMapper extends Mapper<LongWritable, Text, Text, Text> {

	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int firstTabIndex = value.find("\t");
	        int secondTabIndex = value.find("\t", firstTabIndex + 1);

	        // from
	        String from = Text.decode(value.getBytes(), 0, firstTabIndex);
	        // from + rank + tab
	        String fromAndRank = Text.decode(value.getBytes(), 0, secondTabIndex + 1);
	        
	        // mark existence
	        context.write(new Text(from), new Text("!"));

	        // Skip pages with no links.
	        if(secondTabIndex == -1) {
	        	return;
	        }
	        
	        String links = Text.decode(value.getBytes(), secondTabIndex+1, value.getLength() - (secondTabIndex + 1));
	        String[] allToPages = links.split(",");
	        int linksNum = allToPages.length;
	        
	        for (String to : allToPages){
	            Text valuePart = new Text(fromAndRank + linksNum);
	            context.write(new Text(to), valuePart);
	        }
	        
	        
	        // remember links
	        context.write(new Text(from), new Text("|" + links));
	    }
}
