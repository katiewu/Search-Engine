package indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class urlReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		StringBuilder sb = new StringBuilder();
		for (Text val : values) {
			String docID = val.toString();
			if(sb.length() == 0) sb.append(docID);
			else{
				sb.append("\t");
				sb.append(docID);
			}
		}
		String result = new String(sb);
		context.write(key, new Text(result));
	}

}
