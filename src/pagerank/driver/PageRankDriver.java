package pagerank.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pagerank.job1.InitMapper;
import pagerank.job1.InitReducer;
import pagerank.job2.RankingMapper;
import pagerank.job2.RankingReducer;
import pagerank.job3.ResultMapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRankDriver extends Configured implements Tool {


	    private static NumberFormat nf = new DecimalFormat("00");
	    private static int RankIterationTimes = 5;

	    public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Configuration(), new PageRankDriver(), args));
	    }

	    
	    @Override
	    public int run(String[] args) throws Exception {
	        boolean isCompleted = runInitJob("pagerank/in", "pagerank/ranking/00");
	        if (!isCompleted) {
	        	return 1;
	        }

	        String resultPath = null;

	        for (int i = 0; i < RankIterationTimes; i++) {
	            String inPath = "pagerank/ranking/" + nf.format(i);
	            resultPath = "pagerank/ranking/" + nf.format(i + 1);

	            isCompleted = runRankingJob(inPath, resultPath);

	            if (!isCompleted) {
	            	return 1;
	            }
	        }

	        isCompleted = runResultJob(resultPath, "pagerank/result");

	        if (!isCompleted) {
	        	return 1;
	        }
	        
	        return 0;
	    }


	    public boolean runInitJob(String inputPath, String outputPath) throws Exception {
	        Configuration conf = new Configuration();

	        Job initJob = Job.getInstance(conf, "initJob");
	        
	        initJob.setJarByClass(PageRankDriver.class);
	        initJob.setMapOutputKeyClass(Text.class);
	        initJob.setMapOutputValueClass(Text.class);


	        initJob.setOutputKeyClass(Text.class);
	        initJob.setOutputValueClass(Text.class);

	        initJob.setMapperClass(InitMapper.class);
	        initJob.setReducerClass(InitReducer.class);

	        FileInputFormat.setInputPaths(initJob, new Path(inputPath));
	        FileOutputFormat.setOutputPath(initJob, new Path(outputPath));

	        initJob.setInputFormatClass(KeyValueTextInputFormat.class);
	        initJob.setOutputFormatClass(TextOutputFormat.class);

	        return initJob.waitForCompletion(true);
	    }
	    

	    private boolean runRankingJob(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job rankingJob = Job.getInstance(conf, "rankingJob");
	        rankingJob.setJarByClass(PageRankDriver.class);
	        
	        rankingJob.setMapOutputKeyClass(Text.class);
	        rankingJob.setMapOutputValueClass(Text.class);

	        rankingJob.setOutputKeyClass(Text.class);
	        rankingJob.setOutputValueClass(Text.class);

	        FileInputFormat.setInputPaths(rankingJob, new Path(inputPath));
	        FileOutputFormat.setOutputPath(rankingJob, new Path(outputPath));

	        rankingJob.setMapperClass(RankingMapper.class);
	        rankingJob.setReducerClass(RankingReducer.class);

	        return rankingJob.waitForCompletion(true);
	    }

	    private boolean runResultJob(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job resultJob = Job.getInstance(conf, "resultJob");
	        resultJob.setJarByClass(PageRankDriver.class);

	        resultJob.setOutputKeyClass(FloatWritable.class);
	        resultJob.setOutputValueClass(Text.class);

	        resultJob.setMapperClass(ResultMapper.class);

	        FileInputFormat.setInputPaths(resultJob, new Path(inputPath));
	        FileOutputFormat.setOutputPath(resultJob, new Path(outputPath));

	        resultJob.setInputFormatClass(TextInputFormat.class);
	        resultJob.setOutputFormatClass(TextOutputFormat.class);

	        return resultJob.waitForCompletion(true);
	    }

	}
