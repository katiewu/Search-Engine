/**
 * 
 */
package DynamoDB;

import java.nio.ByteBuffer;
import java.util.Arrays;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.sun.org.apache.bcel.internal.generic.NEW;

/**
 * @author dichenli
 * data of page idf
 */
@DynamoDBTable(tableName="IDF")
public class IDF {
	String word; 
	double idf; //page idf
	
	@DynamoDBHashKey(attributeName="word")
    public String getId() { return word; }
	
    
    @DynamoDBAttribute(attributeName="idf")
    public double getidf() { return idf; }    

    
    @Override
    public String toString() {
       return word + "\t" + idf;
    }
    
    public static IDF parseInput(String line) {
		if(line == null) {
			System.out.println("null line");
			return null;
		}
		
		String[] splited = line.split("\t");
		if(splited == null || splited.length != 2) {
			System.out.println("bad line: " + line);
			return null;
		}
		double idf;
		try {
			idf = Double.parseDouble(splited[1]);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		
		String word = splited[0];
		if(word.equals("")) {
			System.out.println("Empty line: " + line);
			return null;
		}
		
		IDF item = new IDF();
		item.word = word;
		item.idf = idf;
		return item;
	}
    
    public static IDF load(String word) {
    	return DynamoTable.mapper.load(DynamoDB.IDF.class, word);
    }
	
    public static void main(String[] args) {
    	System.out.println(load("the"));
    }
}
