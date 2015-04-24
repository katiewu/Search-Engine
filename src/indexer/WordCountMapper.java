package indexer;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;

import snowballstemmer.PorterStemmer;

public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

	private static final String DELIMATOR = " \t\n\r\"'-/.,:;|{}[]!@#%^&*()<>=+`~?";
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
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
    
	public void map(Text key, Text value, Context context) {
		String line = value.toString().toLowerCase();
		line = html2text(line);
		line = stemContent(line);
		HashSet<String> wordSet = new HashSet<String>();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
        	wordSet.add(tokenizer.nextToken());
        }
        for(String w:wordSet){
        	word.set(w);
        	try {
				context.write(word, one);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
        }
	}

}
