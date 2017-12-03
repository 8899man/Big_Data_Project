package edu.itu.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;

public class DataValidator extends Configured implements Tool {

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

    public static class ValidatorMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        String previousLine = null;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value != null
                    && value.toString().length() > 0
                    && value.toString().contains(delimiter)) {
                String currentLine = value.toString();
                if (previousLine != null
                        && currentLine != null) {
                    compare(previousLine, currentLine, context);
                }
                previousLine = currentLine;
            } else {
                throw new InterruptedIOException("Invalid data error");
            }
        }

        private void compare(String previousLine, String currentLine, Context context) throws InterruptedIOException {
            String[] previousFields = previousLine.split(delimiter);
            String[] currentFields = currentLine.split(delimiter);
            if (currentFields[0].compareTo(previousFields[0]) < 0) {
                throw new InterruptedIOException("Not valid values order: " + previousFields[0] + ", " + currentFields[0]);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        initArgs(args);
        Job job = Job.getInstance(getConf());
        job.setJobName("edu.itu.hadoop.DataValidator");
        job.setJarByClass(DataValidator.class);
        FileInputFormat.setInputPaths(job, new Path(in_file_name));
        FileOutputFormat.setOutputPath(job, new Path(out_file_name));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(ValidatorMapper.class);
        job.setNumReduceTasks(0);

        boolean status = false;
        status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long start_ms = System.currentTimeMillis();
        int result = ToolRunner.run(new Configuration(), new DataValidator(), args);
        long finish_ms = System.currentTimeMillis();
        System.out.println("validate time: " + (finish_ms - start_ms)/1000 + " seconds");
        System.exit(result);
    }}
