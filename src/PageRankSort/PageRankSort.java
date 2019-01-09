package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

public class PageRankSort {

    public PageRankSort(){

    }

	public void PageRankSort(String input_path, String output_path, String num_reducer) throws Exception {
        Configuration conf = new Configuration();
        Double double_initial_value = (2.0/(double)Page_Rank.total_page);
        conf.set("double_initial_value", Double.toString(double_initial_value));
		
		Job job = Job.getInstance(conf, "PageRankSort");
        job.setJarByClass(PageRankSort.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(PageRankSortMapper.class);
		job.setPartitionerClass(PageRankSortPartitioner.class);
		job.setReducerClass(PageRankSortReducer.class);
        
        //set the input
        job.setInputFormatClass(KeyValueTextInputFormat.class);
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(SortPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set the number of reducer
		job.setNumReduceTasks(Integer.valueOf(num_reducer));
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);
	}
}