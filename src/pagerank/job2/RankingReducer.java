package pagerank.job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RankingReducer extends Reducer<Text, Text, Text, Text> {

    private static final float damping = 0.85F;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExisting = false;
        String[] split;
        float sum = 0;
        String links = "";
        String valueStr;
        
        // For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        for (Text value : values){
            valueStr = value.toString();
            
            if(valueStr.equals("!")) {
                isExisting = true;
                continue;
            }
            
            if(valueStr.startsWith("|")){
                links = "\t"+valueStr.substring(1);
                continue;
            }

            split = valueStr.split("\\t");
            
            float pageRank = Float.valueOf(split[1]);
            int linksNum = Integer.valueOf(split[2]);
            
            sum += (pageRank / linksNum);
        }

        if(!isExisting) return;
        float newRank = damping * sum + (1-damping);

        context.write(key, new Text(newRank + links));
    }
}