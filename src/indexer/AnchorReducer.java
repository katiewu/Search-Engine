package indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnchorReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// key: word, value: type\tdocID
		// process values
		StringBuilder sb = new StringBuilder();
		for (Text val : values) {
			String info = val.toString();
			sb.append(info);
			sb.append("\t");
		}
		String result = new String(sb);
		context.write(key, new Text(result));
	}

}
