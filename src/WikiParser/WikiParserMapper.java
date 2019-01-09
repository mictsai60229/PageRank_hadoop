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



public class WikiParserMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static enum TotalPageCounter {
        TOTALPAGE
	}
	public static String ExistToken = "&gt;";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
         	
		/*  Match title pattern */  
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(value.toString());
		titleMatcher.find();
		Text title = new Text(unescapeXML(titleMatcher.group(1)));
		// Matcher titleMatcher = titlePattern.matcher(xxx);
		// No need capitalizeFirstLetter
		/*  Match link pattern */
		Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher(value.toString());
		// Matcher linkMatcher = linkPattern.matcher(xxx);
		// Need capitalizeFirstLetter

		context.write(title, new Text(ExistToken));
		while(linkMatcher.find()){
			String s = capitalizeFirstLetter(unescapeXML(linkMatcher.group(1)));
			if(s.isEmpty()) continue;
	
			context.write(new Text(s), title);
		}
		

		context.getCounter(TotalPageCounter.TOTALPAGE).increment(1);
	
	}
	
	private String unescapeXML(String input) {

		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");
    }

    private String capitalizeFirstLetter(String input){

    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
    }
}
