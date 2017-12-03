package edu.itu.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataSortPartitioner extends
        Partitioner<KeyWritable, NullWritable> {
    @Override
    public int getPartition(KeyWritable key, NullWritable value,
                            int numReduceTasks) {
        return (Integer.valueOf(key.getPrimaryKey().charAt(0)) % numReduceTasks);
    }
}
