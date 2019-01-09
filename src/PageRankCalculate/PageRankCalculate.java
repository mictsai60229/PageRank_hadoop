package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.lang.Long;

public class PageRankCalculate{
	
	public PageRankCalculate(){

  	}
	
	public void PageRankCalculate(String input_path, String output_path, String num_reducer) throws Exception {
		Configuration conf = new Configuration();
        conf.setInt("total_page", Page_Rank.total_page);
        conf.set("sum_dangling_page", Page_Rank.sum_dangling_page.toString());
		
		Job job = Job.getInstance(conf, "PageRankCalculate");
		job.setJarByClass(PageRankCalculate.class);		
		
		// set the class of each stage in mapreduce
        job.setMapperClass(PageRankCalculateMapper.class);
		job.setReducerClass(PageRankCalculateReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
        // set the number of reducer
        //using mapper and combiner to calculate sum
		job.setNumReduceTasks(Integer.valueOf(num_reducer));
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);


        // total links?
        Long sum_page_diff_long = job.getCounters()
                .findCounter(PageRankCalculateReducer.CalculateCounter.SUMPAGEDIFF)
                .getValue();
		Page_Rank.sum_page_diff = (double) sum_page_diff_long / Math.pow(2, 63);


		Long sum_dangling_page = job.getCounters()
		.findCounter(PageRankCalculateReducer.CalculateCounter.DANGLENODESUM)
		.getValue();

		Page_Rank.sum_dangling_page = (double) sum_dangling_page / Math.pow(2, 63) / (double) Page_Rank.total_page ;
		
	}
}