package indexer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	{
		System.out.println("initialize mapper");
	}

	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
//    private Text one = new Text("1");
    
	public void map(LongWritable key, Text value, Context context) {
		System.out.println("key "+key+" value "+value);
		String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            try {
            	System.out.println("context write "+word+"  "+one);
				context.write(word, one);
			} catch (IOException | InterruptedException e) {
				
				e.printStackTrace();
			}
        }
	}

}
