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

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.io.*;



public class DangleNodeSumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	// Combiner implements method in Reducer
	
	public DoubleWritable out = new DoubleWritable();
	public long output_value;
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		for (DoubleWritable val: values) {
            sum += val.get();
		}

		//just for debug, but this is useless
		out.set(sum);
		context.write(key, out);
	}
}