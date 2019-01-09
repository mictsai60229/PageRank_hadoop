package page_rank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
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



public class DangleNodeSumMapper extends Mapper<Text, Text, Text, DoubleWritable> {

    public static Text SUM_SIGNAL = new Text("0");
    public DoubleWritable page_rank_value = new DoubleWritable();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {


        String string_value = value.toString();
        int TabIndex = string_value.indexOf("\t");

        if( string_value.length() == (TabIndex+1) ){
            page_rank_value.set(Double.parseDouble(string_value.substring(0, TabIndex)));
            context.write(SUM_SIGNAL, page_rank_value);
        }
	
    }
}