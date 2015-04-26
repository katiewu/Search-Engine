package SearchInterface;



import java.io.IOException;
import java.io.PrintWriter;
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
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		out.println("<html><body>");
		out.println("<form action=\"\" method=\"POST\">");
		out.println("<input type=\"text\" name=\"search\">");
		out.println("<br><input type=\"submit\" value=\"Search\">");
		out.println("</form>");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		String query = request.getParameter("search");
		
	}

}

