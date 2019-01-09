package page_rank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.io.*;



public class PageRankCalculateMapper extends Mapper<Text, Text, Text, Text> {

    public static String SplitToken = "&lt;";


	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
        String string_value = value.toString();
        int TabIndex = string_value.indexOf("\t");

        //not dangle node
        if( string_value.length() > (TabIndex+1) ){
            double page_rank_value = Double.parseDouble(string_value.substring(0, TabIndex));
            String[] links = string_value.substring(TabIndex+1).split(SplitToken);
            double output_value = page_rank_value /(double)links.length;
            Text output_text = new Text(Double.toString(output_value));
            for (String link : links)
            {
                context.write(new Text(link), output_text);
            }
        }
        context.write(key, value);
	}
}