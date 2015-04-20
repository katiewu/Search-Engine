package indexer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	{
		System.out.println("initialize reducer");
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("reduce  key: "+key+" value: ");
		int sum = 0;
        for(IntWritable value:values){
        	sum += value.get();
        }
        context.write(key, new IntWritable(sum));
	}

}
