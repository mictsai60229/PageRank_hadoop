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



public class WikiLinkReverseMapper extends Mapper<Text, Text, Text, Text> {

    public static String SplitToken = "&lt;";
    public static String ExistToken = "&gt;";

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
        String string_value = value.toString();

        context.write(key, new Text(ExistToken));
        if(string_value.isEmpty()) return;
        
		String[] pages = string_value.split(SplitToken);
		for (String page : pages){
            if(page.isEmpty()) continue; 
            context.write(new Text(page), key);
        }
        
	}
}