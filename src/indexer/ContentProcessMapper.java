package indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;

import snowballstemmer.PorterStemmer;

public class ContentProcessMapper extends Mapper<Text, Text, Text, Text> {
	
	private static final String PARSER = " \t\n\r";
	private static final double a = 0.4;
	
	public static String html2text(String content) {
	    return Jsoup.parse(content).text();
	}
	
	public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, PARSER);
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
		StringTokenizer tokenizer = new StringTokenizer(content, PARSER);
		String word = "";
		int max = 0;
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			boolean flag = false;
			for(int i=0;i<word.length();i++){
				if (Character.UnicodeBlock.of(word.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN) {
					flag = true;
					break;
				}
			}	
			if(flag) continue;
			int i = 0;
			while(i<word.length() && ( !Character.isLetter(word.charAt(i)) && !Character.isDigit(word.charAt(i)) )){
				i++;
			}
			if(i>=word.length()) continue;
			word = word.substring(i);
			i = word.length()-1;
			while(i>=0 && ( !Character.isLetter(word.charAt(i)) && !Character.isDigit(word.charAt(i)) )){
				i--;
			}
			if(i<0) continue;
			word = word.substring(0, i+1);
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
		if(!wordSet.containsKey("the")) System.out.println(docID);
	}

}

class WordInfo{
	int size;
	String positionList;
	double tf;
	public WordInfo(){
		this.size = 0;
		positionList = "";
		tf = 0;
	}
	
	public int getSize(){
		return size;
	}
	
	public String getPositions(){
		return positionList;
	}
	
	public double getTF(){
		return tf;
	}
	
	public void addPosition(int position){
		if(!positionList.equals("")) positionList += ",";
		positionList += position;
		size++;
	}
	
	public void setTF(double tf){
		this.tf = tf;
	}
}
