package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class WikiParser{
	
	public WikiParser(){

  	}
	
	public void WikiParser(String input_path, String output_path, String num_reducer) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "WikiParser");
		job.setJarByClass(WikiParser.class);		
		
		// set the class of each stage in mapreduce
		job.setMapperClass(WikiParserMapper.class);
		job.setReducerClass(WikiParserReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(Integer.valueOf(num_reducer));
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);


		// total links?
		Page_Rank.total_page = (int) job.getCounters()
				.findCounter(WikiParserMapper.TotalPageCounter.TOTALPAGE)
				.getValue();

		
	}
	
}