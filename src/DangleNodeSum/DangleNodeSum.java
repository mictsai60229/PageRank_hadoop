package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import page_rank.DangleNodeSumMapper;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.lang.Long;

public class DangleNodeSum{
	
	public DangleNodeSum(){

  	}
	
	public void DangleNodeSum(String input_path, String output_path, String num_reducer) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("total_page", Page_Rank.total_page);
		
		Job job = Job.getInstance(conf, "DangleNodeSum");
		job.setJarByClass(DangleNodeSum.class);		
		
		// set the class of each stage in mapreduce
        job.setMapperClass(DangleNodeSumMapper.class);
        job.setCombinerClass(DangleNodeSumCombiner.class);
		job.setReducerClass(DangleNodeSumReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
        // set the number of reducer
        //using mapper and combiner to calculate sum
		job.setNumReduceTasks(1);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);


		// return value of dangle node
		Long sum_dangling_page = job.getCounters()
		.findCounter(DangleNodeSumReducer.DangleNodeCounter.DANGLENODESUM)
		.getValue();

		Page_Rank.sum_dangling_page = (double) sum_dangling_page / 1000000000000000000L / Page_Rank.total_page ;
	}
	
}