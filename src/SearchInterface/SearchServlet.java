package SearchInterface;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
		out.println("<br><input type=\"submit\" value=\"Search\">");
		out.println("</form>");
		out.println("</body></html>");
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
		out.println("<h1>Result</h1>");
		for(String w:parseQuery){
			out.println("<li>"+w+"</li>");
		}
		out.println("</body></html>");
		
		
	}

}

