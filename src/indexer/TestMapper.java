package indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jsoup.Jsoup;

import snowballstemmer.PorterStemmer;

public class TestMapper extends Mapper<Text, Text, Text, Text> {

	private static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	private static final double a = 0.4;
	
	public static String html2text(String content) {
	    return Jsoup.parse(content).text();
	}
	
	public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		StringBuilder sb = new StringBuilder();
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken(); 
			if(word.equals("")) continue;
			stemmer.setCurrent(word);
			if(stemmer.stem()){
				sb.append(stemmer.getCurrent());
				sb.append(" ");
			}
		}
		return new String(sb);
	}
	
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// key: docID, value: content
		String docID = key.toString();
		String[] val = value.toString().split("\n");
		if(val.length != 2) return;
		String content = val[1].toLowerCase();
		content = html2text(content);

		// stemming the content
		content = stemContent(content);
		
		// process one document
		HashMap<String, WordInfo> wordSet = new HashMap<String, WordInfo>();
		int position = 0;
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		int max = 0;
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			if(!wordSet.containsKey(word)){
				WordInfo info = new WordInfo();
				wordSet.put(word, info);
			}
			wordSet.get(word).addPosition(position);
			int size = wordSet.get(word).getSize();
			if(size>max) max = size;
			position++;
		}
		for(String w:wordSet.keySet()){
			double tf = a + (1-a)*wordSet.get(w).getSize()/max;
			wordSet.get(w).setTF(tf);
			WordInfo wi = wordSet.get(w);
			String wordinfo = docID+"\t"+tf+"\t"+wi.positionList;
			context.write(new Text(w), new Text(wordinfo));
		}
	}
}


