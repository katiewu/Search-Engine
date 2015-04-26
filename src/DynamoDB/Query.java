/**
 * 
 */
package DynamoDB;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

/**
 * @author dichenli
 *
 */
public class Query<T> {
	
	public static DynamoDBMapper mapper;
	
	public Query(DynamoDBMapper m) {
		mapper = m;
	}
	
	public T get(Class<T> classs, Object key) {
		return mapper.load(classs, key);
	}
}
