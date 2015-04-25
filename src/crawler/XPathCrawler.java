package edu.upenn.cis455.crawler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ProtocolException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.validator.UrlValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.tidy.Tidy;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.Channel;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.RawFile;
import edu.upenn.cis455.storage.User;
import edu.upenn.cis455.xpathengine.XPathEngineFactory;
import edu.upenn.cis455.xpathengine.XPathEngineImpl;



public class XPathCrawler {
	
	String directory;
	double maxSize;
	String startUrl;
	int maxFileN;
	DBWrapper db;
	RobotsTxtInfo robots;
	int count = 0;
	
	public static void main(String[] args){
//		args[0] = "https://dbappserv.cis.upenn.edu/crawltest.html";
//		args[1] = "/Users/peach/Documents/cis555/database/";
//		args[2] = "1";
//		args[3] = "3";
		
		
		if(args.length <3 || args.length>4){
			usage();
		}
		else{
			XPathCrawler crawler = new XPathCrawler();
			crawler.initialize(args);
			
			
			
		}
		
	}
	
	public void initialize(String[] args){
		startUrl = args[0];
		directory = args[1];
		db = new DBWrapper(directory);
		robots = new RobotsTxtInfo();
		
		maxSize = Double.parseDouble(args[2]);
		
		
		
		
		Channel rss = new Channel("RSS aggregator1");
		User admin = new User("admin");
		admin.setPassword("admin");
		rss.setUserName("admin");
		rss.addXpath("/rss/channel/item/title[contains(text(),\"war\")]");
		rss.addXpath("/rss/channel/item/title[contains(text(),\"peace\")]");
		rss.addXpath("/rss/channel/item/description[contains(text(),\"war\")]");
		rss.addXpath("/rss/channel/item/description[contains(text(),\"peace\")]");
		db.addUser(admin);
		
		db.addChannel(rss);
		db.addChannelToUser("RSS aggregator", "admin");
		if(args.length == 4){
			maxFileN = Integer.parseInt(args[3]);
		}
		
		db.initialLizeUrlQ(args[0]);
		int i = 0;
		while(i<maxFileN && !db.isQEmpty()){
			String currentUrl = db.getFstUrlFromQ();

			System.out.println("\n\n\n"+i+"th url: "+currentUrl+"\n");
			run(currentUrl);
			i++;
			if(db.isQEmpty()){
				System.out.println("empty!!!");
			}
		}
		db.closeEnv();
	}

	public void run(String currentUrl) {
		NewHttpClient client = new NewHttpClient(currentUrl);
		NewHttpClient getClient = new NewHttpClient(currentUrl);
		String host = client.getHostName();
		String path = client.getPath();
		String protocol = client.getProtocol();
		String type = null, body = null;
		String docid = null;
		int len = -1;
		int port = client.getPort();
		int statusCode = -1;
		long lastModified;
		long lastCrawled;
		//construct a doc id for this url
		docid = String.valueOf(toBigInteger(currentUrl));
//		System.out.println("doc id is "+docid);
		if(!db.containsUrl(docid)){
//			System.out.println("db does not contain this doc id");
			
			db.addDocID(docid);
			db.addDocUrl(docid, currentUrl);
			
			
		}
		else{
//			System.out.println("*****************\ndb contains this doc id!\n**************");
		}
		
		if(!robots.containsHost(host)){
//			System.out.println("no robots info about this host:"+host);
			String robot_url = protocol+"://"+host+"/robots.txt";
			NewHttpClient robotClient = new NewHttpClient(robot_url);
			int robot_sc = -1;
			robotClient.setRequestMethod("GET");
			robot_sc = robotClient.executeMethod();
			

			
				if(robot_sc!=200){
//					System.out.println("failed when retrieving robots.txt!");
				}
				
				else{
					Calendar now = Calendar.getInstance();
					robots.updateLstCrawled(host, now);
					String robotBody = null;
					try {
						robotBody = robotClient.getResponseBody();
					} catch (Exception e) {
					
						e.printStackTrace();
					}
					processRobotTxt(host, robotBody, robots);
				} 
			
				if(robot_sc != 1000){
		    		try {
						robotClient.releaseConnection();
//						System.out.println("close robots client connection!");
					} catch (IOException e) {
						
						e.printStackTrace();
					}
		    	}
			
			
			
			
		}
		
		if(robots.isAllowed(host, path)){
//			System.out.println("allowed and unique url!");
			if(!db.hasSentHead(docid)){
				try {
					//TODO if delayed, put it at the end of the url Q
//					if(robots.delayContainHost(host)){
//						Calendar lst = robots.getLstCrawled(host);
//						Calendar now = Calendar.getInstance();
//						int delay = robots.getCrawlDelay(host);
//						lst.add(Calendar.SECOND, delay);
//						if(now.before(lst)){
//							try {
//								Thread.sleep((long)delay*1000);
//							} catch (InterruptedException e) {
//								
//								e.printStackTrace();
//							}
//						}
//						
//					}
					client.setRequestMethod("HEAD");
					statusCode = client.executeMethod();
					Calendar now = Calendar.getInstance();
					robots.updateLstCrawled(host, now);
//					System.out.println("head request code is "+statusCode);
					
					
					if(statusCode == 200){
						db.sentHead(docid);
						type = client.getContentType();
						len = client.getContentLen();
//						System.out.println("head request success: content_type is "+type
//								+"\ncontent_length is "+len/1024.0/1024.0+"MB");
					}
	//				else if(statusCode == 301 || statusCode == 302){
	//					System.out.println("moved to "+client.getRedirectLocation());
	//					
	//					db.addUrlToQueue(client.getRedirectLocation());
	//				}
					if(statusCode != 1000){
//						System.out.println("head request closed");
							client.releaseConnection();
			    	}
				} catch (IOException e) {
					
					e.printStackTrace();
	//			} catch (InterruptedException e) {
	//				
	//				e.printStackTrace();
				}
			}
			else{
//				System.out.println("has sent head...go ahead to get request..");
				statusCode = 200;
			}
	    	
			if(statusCode >= 200 && statusCode < 400){
				boolean needDelay = false;
				if(checkSizeType(type, len) || (statusCode >200 && statusCode <400)){
					int statusCode_get = -1;
					try {	
						lastModified = client.lastModified;
						if(db.needDownload(currentUrl, lastModified)){
							//TODO delay put it at the end of the url Q
							if(robots.delayContainHost(host)){
								Calendar lst = robots.getLstCrawled(host);
								Calendar now = Calendar.getInstance();
								int delay = robots.getCrawlDelay(host);
								lst.add(Calendar.SECOND, delay);
								if(now.before(lst)){
									db.addUrlToQueue(currentUrl);
									needDelay = true;
									
								}
								
							}
							
							//TODO
							/**
							 * if no delay is needed, then get request for this url
							 * */
							if(!needDelay){
								getClient.setRequestMethod("GET");
								statusCode_get = getClient.executeMethod();
								Calendar now = Calendar.getInstance();
								robots.updateLstCrawled(host, now);
								count++;
								System.out.println("\n****************\n"+count+" url: "+currentUrl+"downloading...\n");
							
//								System.out.println("get request code is "+statusCode_get);
								if(statusCode_get == 200){
//									System.out.println("get request success");
									
									body = getClient.getResponseBody();
									RawFile tmp = new RawFile("");
									tmp.setFileUrl(currentUrl);
									tmp.setFile(body);
									tmp.updateTime();
									db.addRawFile(tmp);
									
									
									
									if(db.contentIsEmpty(docid)){
//										System.out.println("this doc id contains an empty content\nputting content now...");
										db.addDocContent(docid, body, currentUrl);
									}
									
								}
								
	
								
								if(statusCode_get == 200 || (statusCode_get == -1 && db.hasUrlInDB(currentUrl))){
//									System.out.println("prepares to extract links...");
									if(type.equals("text/html")){
										extractLinks(currentUrl);
									}
								}
							}
						
						
						}
						
					}catch (Exception e) {
						
						e.printStackTrace();
					}
					if(statusCode_get != 1000){
						try {
							client.releaseConnection();
						} catch (IOException e) {
							
							e.printStackTrace();
						}
					}
				}
				else{
//					System.out.println("illegal content or type ");
				}
			}
			else{
//				System.out.println("head request failed");
			}
		}
		else{
//			System.out.println("not allowed url or duplicate url!");
		}
		
		
			
		
	}
	
	public void setDB(DBWrapper db){
		this.db = db;
	}

	public void extractLinks(String currentUrl) {
//		System.out.println("\nin the method of extrac links...\n");
		String docid = String.valueOf(toBigInteger(currentUrl));
//		System.out.println("the current url is"+currentUrl+" and its doc id is"+docid);
		
		String docString = db.getFile(currentUrl);
		String baseUrl = currentUrl.substring(0,currentUrl.lastIndexOf("/")+1);
		Document doc = null;
		doc = Jsoup.parse(docString, baseUrl);
		Elements links = doc.select("a[href]");
		for (Element link : links) {
			String url = link.attr("abs:href");
//            System.out.println(" "+url+"  "+link.text());
			String[] schemes = {"http","https"}; // DEFAULT schemes = "http", "https", "ftp"
			UrlValidator urlValidator = new UrlValidator(schemes);
			if (urlValidator.isValid(url)) {
			   
			   if(url.contains("#")){
					url = url.substring(0,url.indexOf("#"));
				}
	            String linkid = String.valueOf(toBigInteger(url));
//	            System.out.println("the current link is"+url+" and its doc id is"+linkid);
	            db.addDocLink(docid, linkid);
	            
	            
	            
				if(!db.containsUrl(linkid)){
//					System.out.println("db does not contain this doc id");
					db.addDocID(linkid);
					db.addDocUrl(linkid, url);
					db.addUrlToQueue(url);
					
				}
			} else {
//			   System.out.println("url is invalid");
			}
			
			
            
        }
		
	}

	public void setMaxSize(Double size){
		maxSize = size;
	}
	
	public boolean checkSizeType(String type, int len) {
		String content_type = type;
		int content_length = len;
		boolean tp = false;
		if(content_type == null) return false;
		else if(content_type.equals("text/html") || content_type.contains("xml")){
			tp = true;
		}
		if(content_length/1024.0/1024.0 > maxSize) return false;
		return tp;
	}

	public void processRobotTxt(String host, String body, RobotsTxtInfo robots) {
//		System.out.println("processing robots info....");
		int cisCrawlerIdx = body.indexOf("User-agent: cis455crawler");
		int starIdx = body.indexOf("User-agent: *");
		StringBuilder sb = new StringBuilder();
		if(cisCrawlerIdx!= -1){
			body = body.substring(cisCrawlerIdx);
			processHelper(body, host, robots);
		}
		else if(starIdx != -1){
//			System.out.println("star index!");
			body = body.substring(starIdx);
			processHelper(body, host, robots);
		}
		
		
	}

	public void processHelper(String body, String host, RobotsTxtInfo robots) {
//		System.out.println("in the method of process helper");
//		System.out.println(body);
		String[] allLines = body.split("\n");
		for(int i = 0; i <allLines.length; i++){
//			System.out.println("line is "+allLines[i]);
			if(allLines[i].length() == 0) break;
			String[] temp = allLines[i].split(": ");
			if(temp[0].equals("Allow")){
				if(temp.length >1){
					robots.addRuleLink(host, temp[1], true);
				}
				
			}
			else if(temp[0].equals("Disallow")){
				if(temp.length>1){
					robots.addRuleLink(host, temp[1], false);
				}
				
			}
			else if(temp[0].equals("Crawl-delay")){
//				System.out.println("crawl-delay is@"+temp[1]+"@");
				if(temp[1] != null){
					robots.addCrawlDelay(host, Integer.parseInt(temp[1].trim()));
				}
			}
		}
		
	}
	
	public BigInteger toBigInteger(String key) {
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
			return new BigInteger(resString, 16);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return new BigInteger("0", 16);
	}

	private static void usage() {
		System.out.println("Please input the following arguments:\n"
				+ "the starting url for crawling\n"
				+ "the directory that holds your db store\n"
				+ "The maximum size, in megabytes, of a document to be retrieved from a Web server\n"
				+ "(optional)the number of files (HTML and XML) to retrieve before stopping\n");
		
	}
}