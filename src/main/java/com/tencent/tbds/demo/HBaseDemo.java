package com.tencent.tbds.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.FileInputStream;
import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseDemo {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage:java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hbase/lib/:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HBaseDemo TableName");
            System.exit(4);
        }
        String tableName = args[0];

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.addResource(new FileInputStream("/opt/cluster_conf/hbase/hbase-site.xml"));

        //add authentication information directly

//        hbaseConf.set("hadoopsecurity_authentication_tbds_username", "bhchen");
//        hbaseConf.set("hbase.security.authentication.tbds.secureid", "2LqMQyyITaGjQqCvmZZLnrCSWvoDSTxd4I2r");
//        hbaseConf.set("hbase.security.authentication.tbds.securekey", "TwIb7Jlg643iL3twgrhZ9dCWaswok8cv");

        System.out.println("\n\ntest read conf file success or not:" + hbaseConf.get("hbase.zookeeper.quorum"));

        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor family = new HColumnDescriptor(toBytes("info"));
        hTableDescriptor.addFamily(family);

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table[" + tableName + "] already exists!");
        } else {
            admin.createTable(hTableDescriptor);
            System.out.println("Create table [" + tableName + "] success!");
        }

        TableName[] tableNames = admin.listTableNames();
        System.out.println("\nTables in HBase:");
        for (TableName name : tableNames) {
            System.out.println(name.getNameAsString());
        }
    }
}
