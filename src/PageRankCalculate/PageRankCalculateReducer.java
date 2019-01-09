package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankCalculateReducer extends Reducer<Text, Text, Text, Text> {

    public static enum  CalculateCounter{
        SUMPAGEDIFF,
        DANGLENODESUM,
        DANGLENODECOUNT
    }

    private static final double alpha = 0.85;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String string_value;
        int TabIndex;
        double sum = 0.0;
        String links = "";
        double old_page_rank_value = 0;
        boolean is_dangle_node = true;
        for(Text value : values){
            
            string_value = value.toString();
            TabIndex = string_value.indexOf("\t");

            if (TabIndex == -1)
            {
                sum += Double.parseDouble(string_value);
            }
            else //old page rank
            {
                // links is <tab><link><split_token><link>
                old_page_rank_value = Double.parseDouble(string_value.substring(0, TabIndex));
                links = string_value.substring(TabIndex);

                //not dangle node
                if( string_value.length() > (TabIndex+1))
                {
                    is_dangle_node = false;
                }
            }
        }


        Double sum_dangling_page = Double.parseDouble(context.getConfiguration().get("sum_dangling_page"));
        int total_page = context.getConfiguration().getInt("total_page", 10000);

        double page_rank_value = (1.0 - alpha) / (double)total_page + alpha * (sum + sum_dangling_page);

        String output_string = Double.toString(page_rank_value)+links;
        context.write(key, new Text(output_string));

        long pagediff = (long)(Math.abs(old_page_rank_value-page_rank_value) * Math.pow(2, 63));
        context.getCounter(CalculateCounter.SUMPAGEDIFF).increment(pagediff);

        if (is_dangle_node)
        {
            long page_rank_value_long = (long) (page_rank_value * Math.pow(2, 63));
            context.getCounter(CalculateCounter.DANGLENODESUM).increment(page_rank_value_long);
            context.getCounter(CalculateCounter.DANGLENODECOUNT).increment(1);
        }

	}
}