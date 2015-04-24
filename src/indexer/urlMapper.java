package indexer;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import snowballstemmer.PorterStemmer;

public class urlMapper extends Mapper<Text, Text, Text, Text> {

	private static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]%!@#^&*()<>=+`~?";
	private static HashSet<String> stopWords = new HashSet<String>();

	static {
		String[] lists = { "edu", "com", "html", "htm", "xml", "php", "org",
				"gov", "net", "int", "jpg", "png", "bmp", "jpeg", "pdf", "asp",
				"aspx" };
		for (String word : lists) {
			stopWords.add(word);
		}
	}

	public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		StringBuilder sb = new StringBuilder();
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if (word.equals(""))
				continue;
			stemmer.setCurrent(word);
			if (stemmer.stem()) {
				sb.append(stemmer.getCurrent());
				sb.append(" ");
			}
		}
		return new String(sb);
	}

	public static boolean isNumber(String s) {
		String pattern = "\\d+";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);
		// Now create matcher object.
		Matcher m = r.matcher(s);
		return m.find();
	}

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// key: docID, value: url
		String url = value.toString().toLowerCase();
		if(url.startsWith("http://")){
			url = url.substring(7);
		}
		else if(url.startsWith("https://")){
			url = url.substring(8);
		}
		if(url.startsWith("www.")){
			url = url.substring(4);
		}
		url = stemContent(url);
		StringTokenizer tokenizer = new StringTokenizer(url, DELIMATOR);
		String word = "";
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("") || word.length()>20) continue;
			if(isNumber(word)) continue;
			if(!stopWords.contains(word)){
				context.write(new Text(word), key);
			}
		}
		
		
	}
}
