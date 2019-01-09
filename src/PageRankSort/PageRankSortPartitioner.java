package page_rank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

public class PageRankSortPartitioner extends Partitioner<SortPair, NullWritable> implements Configurable{

    Configuration config = null;
    double double_initial_value;
    double interval;
    
    @Override
    public int getPartition(SortPair key, NullWritable value, int numReduceTasks) {
    
        if (numReduceTasks == 1) return 0;
        
        double page_rank_value = key.getAverage();
        interval = double_initial_value/numReduceTasks;

        if (page_rank_value >= double_initial_value)
        {
            return 0;
        }

        return numReduceTasks-1-(int)(page_rank_value/interval);

    }
    
    @Override
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return this.config;
    }
    
    @Override
    public void setConf(Configuration configuration) {
        this.config = configuration;
        double_initial_value = Double.parseDouble(this.config.get("double_initial_value"));
     } 
}
    
