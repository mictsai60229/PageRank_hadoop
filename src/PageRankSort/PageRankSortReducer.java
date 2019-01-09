package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;


public class PageRankSortReducer extends Reducer<SortPair, NullWritable, Text, DoubleWritable> {
   
    private DoubleWritable output_average = new DoubleWritable();
    public void reduce(SortPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // output the word and average
        
        Text output_key = key.getWord();
        double average = key.getAverage();
        output_average.set(average);
        context.write(output_key, output_average);

    }
}