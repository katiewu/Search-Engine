package indexer;

import java.util.StringTokenizer;

public class TestParser {
	private static final String PARSER = " \t\n\r";
	
	public static void main(String[] args){
		String content = "Yes, 234the run() method of the mapper is called by the MR framework when running the map task attempt. As far as the context is concerned, take a look at the documentation for Mapper.Context, especially the implemented interfaces and their javadocs give you a full overview of the information contained in the context. Through the context, you can access data like:";
		StringTokenizer tokenizer = new StringTokenizer(content, PARSER);
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
			int i = 0;
			System.out.println("original word "+word);
			while(i<word.length() && ( !Character.isLetter(word.charAt(i)) || !Character.isDigit(word.charAt(i)) )){
				i++;
			}
			if(i>=word.length()) continue;
			word = word.substring(i);
			i = word.length()-1;
			while(i>=0 && !Character.isLetter(word.charAt(i))){
				i--;
			}
			if(i<0) continue;
			word = word.substring(0, i+1);
			System.out.println("insert word "+word);
		}
	}

}
