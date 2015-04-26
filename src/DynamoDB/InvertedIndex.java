/**
 * 
 */
package DynamoDB;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.sun.xml.internal.fastinfoset.algorithm.BuiltInEncodingAlgorithm.WordListener;
/**
 * @author dichenli
 *
 */
@DynamoDBTable(tableName="InvertedIndex")
public class InvertedIndex {
	byte[] id; //binary data, docID
	String word; 
	HashSet<Number> positions; //position of the word in document
	float tf; //TF value
	
	
	public InvertedIndex(String word2, byte[] id2, float tf2,
			HashSet<Number> positions2) {
		this.word = word2;
		this.id = id2;
		this.positions = positions2;
		this.tf = tf2;
	}

	@DynamoDBRangeKey(attributeName="id")
    public ByteBuffer getId() { return ByteBuffer.wrap(id); }
	
    public void setId(ByteBuffer buf) { 
    	this.id = buf.array(); 
    }
    
    public void setId(String hexString) {
    	id = BinaryUtils.fromHex(hexString);
    }
    
    @DynamoDBHashKey(attributeName="word")
    public String getWord() { return word; }    
    
    public void setWord(String word) { this.word = word; }
    
    @DynamoDBAttribute(attributeName="positions")
    public Set<Number> getPositions() {
    	return  positions;
    }
    
    public void setPositions(HashSet<Number> positions) {
		this.positions = positions;
	}
    
    public void addPosition(Integer pos) {
    	positions.add(pos);
    }
    
    @DynamoDBAttribute(attributeName="tf")
    public float getTF() {
    	return tf;
    }
    
    public void setTF(float tf) {
    	this.tf = tf;
    }
    
    @Override
    public String toString() {
       return word + BinaryUtils.byteArrayToString(id);
    }
    
    public static InvertedIndex parseInput(String line) {
    	if (line == null) {
    		System.err.println("parseInput: null line!");
    		return null;
    	}
    	
    	String[] splited = line.split("\t");
    	if (splited.length != 4) {
    		System.err.println("parseInput: bad line: " + line);
    		return null;
    	}
    	
    	String word = splited[0].trim();
    	if (word.equals("")) {
    		System.err.println("parseInput: word empty: " + line);
    		return null;
    	}
    	
    	byte[] id = BinaryUtils.fromHex(splited[1].trim());
    	if (id.length == 0) {
    		System.err.println("parseInput: id wrong: " + line);
    		return null;
    	}
    	
    	float tf;
    	try {
    		tf = Float.parseFloat(splited[2].trim());
    	} catch(Exception e) {
    		System.err.println("parseInput: tf wrong: " + line);
    		return null;
    	}
    	
    	String[] posStrs = splited[3].split(",");
    	if (posStrs.length == 0) {
    		System.err.println("parseInput: positions wrong: " + line);
    		return null;
    	}
    	
    	HashSet<Number> positions = new HashSet<Number>();
    	for (String p : posStrs) {
    		try {
    			Number pos = Integer.parseInt(p);
    			positions.add(pos);
    		} catch(Exception e) {
    			System.err.println("parseInput: positions wrong: " + line);
        		return null;
    		}
    	}
    	
    	return new InvertedIndex(word, id, tf, positions);
    }
}
