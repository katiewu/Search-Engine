package indexer;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import snowballstemmer.PorterStemmer;

public class AnchorMapper extends Mapper<Text, Text, Text, Text> {
	
	private static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	
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
			System.out.println(word); 
			if(word.equals("")) continue;
			stemmer.setCurrent(word);
			if(stemmer.stem()){
				sb.append(stemmer.getCurrent());
				sb.append(" ");
			}
		}
		return new String(sb);
	}
	
	public static void splitKey(String content, String docID, int type, Context context){
		String store_text = stemContent(content);
		StringTokenizer tokenizer = new StringTokenizer(store_text, DELIMATOR);
		String word = "";
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
			// write the word and url to file
			Text key = new Text(word);
			Text value = new Text(type+"\t"+docID);
			try {
				context.write(key, value);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static String toBigInteger(String key) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
			messageDigest.update(key.getBytes());
			byte[] bytes = messageDigest.digest();
			Formatter formatter = new Formatter();
			for (int i = 0; i < bytes.length; i++) {
				formatter.format("%02x", bytes[i]);
			}
			String resString = formatter.toString();
			formatter.close();
			return String.valueOf(new BigInteger(resString, 16));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return String.valueOf(new BigInteger("0", 16));
	}
	
	public static boolean isNumber(String s) {
		String pattern = "\\d+";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);
		// Now create matcher object.
		Matcher m = r.matcher(s);
		return m.find();
	}
	
	public void analURL(String url, String docID, Context context) throws IOException, InterruptedException {
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
				String result = "3\t"+docID;
				context.write(new Text(word), new Text(result));
			}
		}
		
		
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// key: docID, value: url\ncontent
		// 0:anchor, 1:img, 2:title, 3:url
		// each map function process one document
		String docId = key.toString();
		String[] contentInfo = value.toString().split("\n");
		if(contentInfo.length == 2){
			String url = contentInfo[0];
			analURL(url, docId, context);
			String content = contentInfo[1];
			Document doc = Jsoup.parse(content, url);
			// extract anchor text
			Elements links = doc.select("a");
			for (Element link : links) {
				String abshref = link.attr("abs:href");
				String relhref = link.attr("href");
				String title = link.attr("title").toLowerCase();
				String text = link.text().toLowerCase();
				if (abshref.equals("") || relhref.startsWith("#")) {
					// if there is no reference, or reference starts with #, replies
					// jump to fragment
					continue;
				}
				if(abshref.contains("#")){
					int index = abshref.indexOf("#");
					abshref = abshref.substring(0, index);
				}
				String linkID = toBigInteger(abshref);
				splitKey(text + " " + title, linkID, 0, context);
			}
			// extract meta data
			Elements metas = doc.select("meta");
			for(Element meta:metas){
				String metacontent = meta.attr("content").toLowerCase();
				splitKey(metacontent, docId, 1, context);
			}
			// extract title
			Elements titles = doc.select("title");
			for (Element title : titles) {
				String titlecontent = title.text().toLowerCase();
				if (titlecontent.equals(""))
					continue;
				splitKey(titlecontent, docId, 2, context);
			}
		}
		
	}
	
	public static void main(String[] args){
		String x = "https://twitter.com/USTornadoes/status/592004432847609857";
		for(int i=0;i<10;i++){
			System.out.println(toBigInteger(x));
		}
	}

}
