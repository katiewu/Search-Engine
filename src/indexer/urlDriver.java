package indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class urlDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", "\t");
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(urlDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(urlMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(urlReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		

//		KeyValueTextInputFormat.addInputPath(job, new Path("./input"));
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, new Path("./url"));
		FileOutputFormat.setOutputPath(job, new Path("./urloutput"));

		System.out.println("finish set up");
		
		try{
			if (!job.waitForCompletion(true))
				System.out.println("not running");
			else{
				System.out.println("running");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
