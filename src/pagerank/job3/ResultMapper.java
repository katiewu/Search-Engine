package pagerank.job3;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ResultMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pageAndRank = getPageAndRank(key, value);
        
        float parseFloat = Float.parseFloat(pageAndRank[1]);
        
        Text page = new Text(pageAndRank[0]);
        FloatWritable rank = new FloatWritable(parseFloat);

        context.write(rank, page);
    }
    
    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
        String[] pageAndRank = new String[2];
        int firstTabIndex = value.find("\t");
        int secondTabIndex = value.find("\t", firstTabIndex + 1);
        
        // no tab after rank (when there are no links)
        int rankLength;
        if (secondTabIndex == -1) {
        	rankLength = value.getLength() - (firstTabIndex + 1);
        } else {
        	rankLength = secondTabIndex - (firstTabIndex + 1);
        }
        
        pageAndRank[0] = Text.decode(value.getBytes(), 0, firstTabIndex);
        pageAndRank[1] = Text.decode(value.getBytes(), firstTabIndex + 1, rankLength);
        
        return pageAndRank;
    }
    
}
