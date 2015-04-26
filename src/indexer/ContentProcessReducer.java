package indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ContentProcessReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		int size = 0;
		StringBuilder sb = new StringBuilder();
		for(Text val:values){
//			sb.append(val.toString());
//			sb.append("\t");
//			size++;
			context.write(key, val);
		}
//		sb.insert(0, Integer.toString(size)+"\t");
//		String result = new String(sb);
//		// idf
//		context.write(key, new Text(result));
	}

}
