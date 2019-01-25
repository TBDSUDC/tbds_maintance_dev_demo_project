package com.tencent.tbds.demo;

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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

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
        if (args.length < 2) {
            System.out.println("usage: hadoop jar dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.MapReduceDemo inputPath outputPath");
            return;
        }
        Configuration conf = new Configuration();
        conf.addResource(new FileInputStream("/opt/cluster_conf/hadoop/core-site.xml"));
        conf.addResource(new FileInputStream("/opt/cluster_conf/hadoop/mapred-site.xml"));

        //add authentication information directly
//            conf.set("hadoop_security_authentication_tbds_username", "bhchen");
//            conf.set("hadoop_security_authentication_tbds_secureid", "g9q06icsbwYWjQ4i2wbjz3MWNpo8DXqAZxzZ");
//            conf.set("hadoop_security_authentication_tbds_securekey", "qbQyCiWaCJ0HmgiVpc5qofcKd8kVsJgj");

        System.out.println("[hadoop.security.authentication]--->" + conf.get("hadoop.security.authentication"));
        System.out.println("[hadoop_security_authentication_tbds_username]--->" + conf.get("hadoop_security_authentication_tbds_username"));
        System.out.println("[hadoop_security_authentication_tbds_secureid]--->" + conf.get("hadoop_security_authentication_tbds_secureid"));
        System.out.println("[hadoop_security_authentication_tbds_securekey]--->" + conf.get("hadoop_security_authentication_tbds_securekey"));

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);

        Job job = Job.getInstance(conf, "MapReduce-WordCountDemo") ;
        job.setJarByClass(MapReduceDemo.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


