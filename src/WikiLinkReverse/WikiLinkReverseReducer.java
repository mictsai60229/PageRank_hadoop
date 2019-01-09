package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiLinkReverseReducer extends Reducer<Text, Text, Text, Text> {

    public static enum  TotalPageCounter{
        TOTALDANGLEDPAGE
    }
    public static String SplitToken = "&lt;";
    public static String ExistToken = "&gt;";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer output = new StringBuffer();

        boolean NotHead = false;
        int total_page = context.getConfiguration().getInt("total_page", 10000);

        double init_value = (double) 1.0/(double) total_page;
        String Page = key.toString();

        output.append(Double.toString(init_value));
        output.append("\t");
        for(Text val : values){
            
            String v = val.toString();

            if(!v.equals(ExistToken))
            {
                if (NotHead)
                {
                    output.append(SplitToken);
                }
                output.append(v);
                NotHead = true;
            }
        }
        //empty string
        if(!NotHead)
        {
            context.getCounter(TotalPageCounter.TOTALDANGLEDPAGE).increment(1);
        }
        context.write(key, new Text(output.toString()));
	}
}