package indexer;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import snowballstemmer.PorterStemmer;

public class AnchorMapper extends Mapper<Text, Text, Text, Text> {
	
	private static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";

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
	
	public String toBigInteger(String key) {
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
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// key: url, value: content
		// 0:anchor, 1:img, 2:title
		// each map function process one document
		String docId = key.toString();
		String[] contentInfo = value.toString().toLowerCase().split("\n");
		if(contentInfo.length == 2){
			String url = contentInfo[0];
			String content = contentInfo[1];
			Document doc = Jsoup.parse(content, url);
			// extract anchor text
			Elements links = doc.select("a");
			for (Element link : links) {
				String abshref = link.attr("abs:href");
				String relhref = link.attr("href");
				String title = link.attr("title");
				String text = link.text();
				if (abshref.equals("") || relhref.startsWith("#")) {
					// if there is no reference, or reference starts with #, replies
					// jump to fragment
					continue;
				}
				String linkID = toBigInteger(abshref);
				splitKey(text + " " + title, linkID, 0, context);
			}
			// extract img
			Elements imgs = doc.select("img");
			for (Element img : imgs) {
				String imgsrc = img.attr("abs:src");
				String imgalt = img.attr("alt");
				if (imgalt.equals(""))
					continue;
				String imgID = toBigInteger(imgsrc);
				splitKey(imgalt, imgID, 1, context);
			}
			// extract title
			Elements titles = doc.select("title");
			for (Element title : titles) {
				String titlecontent = title.text();
				if (titlecontent.equals(""))
					continue;
				splitKey(titlecontent, docId, 2, context);
			}
		}
		
	}

}
