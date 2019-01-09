package page_rank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;


public class PageRankSortMapper extends Mapper<Text, Text, SortPair, NullWritable> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
        String string_value = value.toString();
        int TabIndex = string_value.indexOf("\t");
        double page_rank_value = Double.parseDouble(string_value.substring(0, TabIndex));
        
        SortPair result = new SortPair(key, page_rank_value);
		context.write(result, NullWritable.get());
	}
	
}