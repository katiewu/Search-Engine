package indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnchorReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// key: word, value: type\tdocID
		// process values
		HashMap<String, List<Integer>> set = new HashMap<String, List<Integer>>();
		for (Text val : values) {
			String info = val.toString();
			String[] contentinfo = info.split("\t");
			int type = Integer.parseInt(contentinfo[0]);
			String docID = contentinfo[1];
			if(!set.containsKey(docID)){
				set.put(docID, new ArrayList<Integer>());
			}
			if(!set.get(docID).contains(type)){
				set.get(docID).add(type);
			}
		}
		for(String docID:set.keySet()){
			List<Integer> lst = set.get(docID);
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<lst.size();i++){
				if(i != 0) sb.append(",");
				sb.append(lst.get(i));
			}
			context.write(key, new Text(docID+"\t"+new String(sb)));
		}
	}
}
