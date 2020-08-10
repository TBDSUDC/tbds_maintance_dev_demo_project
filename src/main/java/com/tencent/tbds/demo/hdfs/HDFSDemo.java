package com.tencent.tbds.demo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 认证方式一：使用环境变量
 *     export hadoop_security_authentication_tbds_secureid=<id>
 *     export hadoop_security_authentication_tbds_securekey=<key>
 *     export hadoop_security_authentication_tbds_username=<user>
 *
 * 认证方式二：通过代码设置认证参数
 *
 * usage:
 * hadoop jar dev-demo-<version>.jar com.tencent.tbds.demo.hdfs.HDFSDemo --auth-user <username> --auth-id <id> --auth-key <key>
 */
public class HDFSDemo {
    public static void main(String[] args) throws Exception {
        HDFSDemoOption option = new HDFSDemoOption(args);
        if (option.hasHelp()) {
            option.printHelp();
            return;
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", option.getDefaultFS());

        // 设置认证参数
        conf.set("hadoop_security_authentication_tbds_username", option.getAuthUser());
        conf.set("hadoop_security_authentication_tbds_secureid", option.getAuthId());
        conf.set("hadoop_security_authentication_tbds_securekey", option.getAuthKey());

        // 将配置强制设置为程序运行时的环境配置
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fileList = fs.listStatus(new Path("/"));
        System.out.println("---------------List all dirs and files under root dir---------------");
        for (FileStatus fileStatus : fileList) {
            System.out.println(fileStatus.toString());
        }
    }
}
