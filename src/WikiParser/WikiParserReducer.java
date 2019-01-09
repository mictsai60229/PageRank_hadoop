package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiParserReducer extends Reducer<Text, Text, Text, Text> {

    public static String SplitToken = "&lt;";
    public static String ExistToken = "&gt;";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer output = new StringBuffer();

        boolean IsPageLink = false;
        boolean NotHead = false;


        String Page = key.toString();

        for(Text val : values){
            
            String v = val.toString();

            if(v.equals(ExistToken)){
                IsPageLink = true;
            }
            else{
                if (NotHead)
                {
                    output.append(SplitToken);
                }
                output.append(v);
                NotHead = true;
            }
        }

        if(!IsPageLink) return;

        context.write(key, new Text(output.toString()));
	}
}