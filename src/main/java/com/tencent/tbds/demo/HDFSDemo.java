package com.tencent.tbds.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileInputStream;
import java.io.IOException;

public class HDFSDemo {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            // add conf files
            conf.addResource(new FileInputStream("/opt/cluster_conf/hadoop/core-site.xml"));
            conf.addResource(new FileInputStream("/opt/cluster_conf/hadoop/hdfs-site.xml"));

            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            //add authentication information directly
    //            conf.set("hadoop_security_authentication_tbds_username", "bhchen");
//            conf.set("hadoop_security_authentication_tbds_secureid", "g9q06icsbwYWjQ4i2wbjz3MWNpo8DXqAZxzZ");
//            conf.set("hadoop_security_authentication_tbds_securekey", "qbQyCiWaCJ0HmgiVpc5qofcKd8kVsJgj");


            System.out.println("check read conf file success:\n[dfs.namenode.http-address]-->" + conf.get("dfs.namenode.http-address"));
            System.out.println("[hadoop.security.authentication]--->" + conf.get("hadoop.security.authentication"));
            System.out.println("[hadoop_security_authentication_tbds_username]--->" + conf.get("hadoop_security_authentication_tbds_username"));
            System.out.println("[hadoop_security_authentication_tbds_secureid]--->" + conf.get("hadoop_security_authentication_tbds_secureid"));
            System.out.println("[hadoop_security_authentication_tbds_securekey]--->" + conf.get("hadoop_security_authentication_tbds_securekey"));

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromSubject(null);

            FileSystem fs = FileSystem.get(conf);

            FileStatus[] fileList = fs.listStatus(new Path("/"));
            System.out.println("---------------List all dirs and files under root dir---------------");
            for (FileStatus fileStatus : fileList) {
                System.out.println(fileStatus.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
