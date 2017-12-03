package edu.itu.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataSorter extends Configured implements Tool {

    public static final String IN_FILE = "in";
    public static final String OUT_FILE = "out";

    private static Map<String, String> argsMap = new HashMap<String, String>();
    private static String delimiter = ",";
    private static String in_file_name;
    private static String out_file_name;

    private void checkArg(String name)  {
        String value = argsMap.get(name);
        if ( value == null || value.length() == 0 ) {
            throw new IllegalArgumentException("Arg " + name + " must be configured.");
        }
    }

    private void initArgs(String[] args) {
        argsMap.clear();
        for (int i = 0; i < args.length; i++) {
            if (IN_FILE.equals(args[i])) {
                argsMap.put(IN_FILE, args[++i]);
            } else if (OUT_FILE.equals(args[i])) {
                argsMap.put(OUT_FILE, args[++i]);
            }
        }
        checkArg(OUT_FILE);
        checkArg(IN_FILE);
        out_file_name = argsMap.get(OUT_FILE);
        in_file_name = argsMap.get(IN_FILE);
    }

    public static class DataSortMapper extends Mapper<LongWritable, Text, KeyWritable, NullWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value != null && value.toString() != null
                    && value.toString().length() > 0) {
                String fields[] = value.toString().split(delimiter);
                KeyWritable compositeKey = new KeyWritable();
                compositeKey.setPrimaryKey(fields[0]);
                compositeKey.setDelimiter(delimiter);
                StringBuilder secondaryKeyValue = new StringBuilder();
                for (int index = 1; index < fields.length; index++) {
                    if (index>1) {
                        secondaryKeyValue.append(delimiter);
                    }
                    secondaryKeyValue.append(fields[index]);
                }
                compositeKey.setSecondaryKey(secondaryKeyValue.toString());
                context.write(compositeKey, NullWritable.get());
            }
        }
    }

    public static class DataSortReducer extends
            Reducer<KeyWritable, NullWritable, KeyWritable, NullWritable> {
        @Override
        public void reduce(KeyWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            for (NullWritable value: values) {
                context.write(key, NullWritable.get());
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        initArgs(args);
        Job job = Job.getInstance(getConf());
        job.setJobName("edu.itu.hadoop.DataSorter");
        job.setJarByClass(DataSorter.class);
        FileInputFormat.setInputPaths(job, new Path(in_file_name));
        FileOutputFormat.setOutputPath(job, new Path(out_file_name));
        job.setMapperClass(DataSortMapper.class);
        job.setMapOutputKeyClass(KeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(DataSortPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setReducerClass(DataSortReducer.class);
        job.setOutputKeyClass(KeyWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        boolean status = false;
        status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long start_ms = System.currentTimeMillis();
        int result = ToolRunner.run(new Configuration(), new DataSorter(), args);
        long finish_ms = System.currentTimeMillis();
        System.out.println("sort time: " + (finish_ms - start_ms)/1000 + " seconds");
        System.exit(result);
    }
}
