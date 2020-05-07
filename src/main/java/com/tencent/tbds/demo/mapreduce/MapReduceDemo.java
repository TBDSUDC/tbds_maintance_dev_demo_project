package com.tencent.tbds.demo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * usage:
 * hadoop jar dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.mapreduce.MapReduceDemo --auth-user <username> --auth-id <id> --auth-key <key> --input <input path> --output <output path>
 *
 */
public class MapReduceDemo {

    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        MapReduceDemoOption option = new MapReduceDemoOption(args);
        if (option.hasHelp()) {
            option.printHelp();
            return;
        }

        Configuration conf = new Configuration();

        // 设置认证参数
        conf.set("hadoop_security_authentication_tbds_username", option.getAuthUser());
        conf.set("hadoop_security_authentication_tbds_secureid", option.getAuthId());
        conf.set("hadoop_security_authentication_tbds_securekey", option.getAuthKey());

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);

        Job job = Job.getInstance(conf, "MapReduce-WordCountDemo");
        job.setJarByClass(MapReduceDemo.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(option.getInput()));
        FileOutputFormat.setOutputPath(job, new Path(option.getOutput()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


