package indexer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	
	{
		System.out.println("initialize reducer");
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
        for(IntWritable value:values){
        	sum += value.get();
        }
        double idf = Math.log(944.0/(sum*1.0));
//        double idf = (double)sum;
        context.write(key, new DoubleWritable(idf));
	}

}
