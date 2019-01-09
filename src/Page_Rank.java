package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Page_Rank{

	public static int total_page;
	public static int num_dangling_page;
	public static Double sum_dangling_page;
	public static Double sum_page_diff;
	private static String NumOfReducer = "32";

	
	public static void main(String[] args) throws Exception {  
	
	/* Don't need to modify this file.
		InputDir : args[0]
		OutputDir : args[1] 
		Number of iterations : args[2] */

	//Job 1: ParseWikiLink
	//template output string
	long startTime = System.nanoTime();

	String WikiLinkOutput = "PageRank/WikiLink";
	WikiParser job1 = new WikiParser();
	job1.WikiParser(args[0], WikiLinkOutput, NumOfReducer);

	//job2 Calculate Danglinging nodes & calculating initial value
	//output result should be word<\t>links<\t>old page rank<\t>new page rank
	String PageRankCurrent = "PageRank/iteration/0000";
	WikiLinkReverse job2 = new WikiLinkReverse();
	job2.WikiLinkReverse(WikiLinkOutput, PageRankCurrent, NumOfReducer);

	//job3 calcualte and converage
	int max_iteration = Integer.parseInt(args[2]);
	int iteration = 0;

	String PageRankNext;
	String DangleNodeOut;
	
	ArrayList<Double> page_diff_list = new ArrayList<Double>(); 

	sum_dangling_page = (double) num_dangling_page / (double)total_page / (double)total_page;

	if (max_iteration > 0)
	{
		while(iteration < max_iteration)
		{
			PageRankNext = String.format("PageRank/iteration/%04d", iteration);
			//sum dangled nodes
			
			//job4 calcuata next iteration error
			PageRankCalculate job4 = new PageRankCalculate();
			PageRankNext = String.format("PageRank/iteration/%04d", iteration+1);
			job4.PageRankCalculate(PageRankCurrent, PageRankNext, NumOfReducer);
			page_diff_list.add(sum_page_diff);

			Configuration conf = new Configuration();
			Path output = new Path(PageRankCurrent);
			FileSystem hdfs = FileSystem.get(conf);
			// delete existing directory
			if (hdfs.exists(output)) {
				hdfs.delete(output, true);
			}

			PageRankCurrent = PageRankNext;
			iteration = iteration + 1;
		}
	}
	else
	{
		sum_page_diff = 1.0;
		while (sum_page_diff >= 0.001 || sum_page_diff < 0)
		{
			//job4 calcuata next iteration error
			sum_page_diff = 0.0;
			PageRankCalculate job4 = new PageRankCalculate();
			PageRankNext = String.format("PageRank/iteration/%04d", iteration+1);
			job4.PageRankCalculate(PageRankCurrent, PageRankNext, NumOfReducer);
			page_diff_list.add(sum_page_diff);

			Configuration conf = new Configuration();
			Path output = new Path(PageRankCurrent);
			FileSystem hdfs = FileSystem.get(conf);
			// delete existing directory
			if (hdfs.exists(output)) {
				hdfs.delete(output, true);
			}

			PageRankCurrent = PageRankNext;
			iteration = iteration + 1;
		}
	}


	//job5 sort
	PageRankSort job5 = new PageRankSort();
	job5.PageRankSort(PageRankCurrent, args[1], NumOfReducer);

	long endTime = System.nanoTime();
	long total = endTime - startTime;

	System.out.printf("Total execution time: %d sec\n", TimeUnit.NANOSECONDS.toSeconds(total));
	for (int i = 0; i < page_diff_list.size(); i++) {
        sum_page_diff = page_diff_list.get(i);
        System.out.printf("Iteration:%d Sum of converage error: %s\n", i+1, sum_page_diff.toString());
    }
	System.out.printf("Number of page: %d\n", total_page);
	System.out.printf("Number of dangling node: %d\n", num_dangling_page);

	
	System.exit(0);
	}  
}
