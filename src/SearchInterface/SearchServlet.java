package SearchInterface;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import DynamoDB.DocURL;
import DynamoDB.IDF;
import DynamoDB.InvertedIndex;
import DynamoDB.PageRank;
import snowballstemmer.PorterStemmer;

/**
 * Servlet implementation class SearchInterface
 */

public class SearchServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static String PARSER = " \t\n\r";

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	
	public static List<String> stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, PARSER);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		List<String> parseQuery = new ArrayList<String>();
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
			stemmer.setCurrent(word);
			if(stemmer.stem()){
				parseQuery.add(word);
			}
		}
		return parseQuery;
	}
	
	
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		out.println("<html><body>");
		out.println("<form action=\"\" method=\"POST\">");
		out.println("<input type=\"text\" name=\"search\">");
		out.println("<br><input type=\"submit\" value=\"Search!!!\">");
		out.println("</form>");
		out.println("</body></html>");
		System.out.println("1");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		String query = request.getParameter("search").toLowerCase();
		List<String> parseQuery = stemContent(query);
		/*
		 * search query
		 */
		out.println("<html><body>");
		out.println("<h1>Result!!!</h1>");
		
		System.out.println("0");
		String word1 = parseQuery.get(0);
		List<InvertedIndex> invertedIndexCollection = InvertedIndex.query(word1);
		ArrayList<ResultType> res = new ArrayList<ResultType>();
		
		for(InvertedIndex item : invertedIndexCollection) {
			float rank = PageRank.load(word1).getRank();
			double idf = IDF.load(word1).getidf();
			float tf = item.getTF();
			String url = DocURL.load(item.getId().array()).getURL();
			res.add(new ResultType(url, rank * idf * tf));
		}
		
		Collections.sort(res, new ResultTypeComparator());
		
		for(int i = 0; i < res.size(); i++) {
			out.println(res.get(i).url);
		}
		
		
		out.println("</body></html>");
		
		
	}
	
	
	
	private class ResultType{
		String url;
		double rank;
		
		private ResultType(String url, double rank) {
			this.url = url;
			this.rank = rank;
		}
		
		private String getURL() {
			return this.url;
		}
		
		private double getRank() {
			return this.rank;
		}
	}
	
	private class ResultTypeComparator implements Comparator<ResultType> {

		@Override
		public int compare(ResultType o1, ResultType o2) {
			return new Double(o2.getRank()).compareTo(o1.getRank());
		}
		
	}

}

