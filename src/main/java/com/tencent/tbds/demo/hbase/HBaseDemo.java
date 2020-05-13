package com.tencent.tbds.demo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 认证方式一：环境变量设置认证
 *     export hbase_security_authentication_tbds_secureid=<id>
 *     export hbase_security_authentication_tbds_securekey=<key>
 *
 * 认证方式二：代码里面设置认证
 *
 * usage:
 * java -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/hbase/lib/* com.tencent.tbds.demo.hbase.HBaseDemo --auth-id <id> --auth-key <key> --zk-host <zookeeper host list> --table-name <table name>
 */
public class HBaseDemo {
    public static void main(String[] args) throws IOException {
        HBaseDemoOption option = new HBaseDemoOption(args);
        if (option.hasHelp()) {
            option.printHelp();
            return;
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", option.getZkHost());
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        // 认证参数
        conf.set("hbase.security.authentication.tbds.secureid", option.getAuthId());
        conf.set("hbase.security.authentication.tbds.securekey", option.getAuthKey());

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName[] tableNames = admin.listTableNames();
        System.out.println("Tables in HBase:");
        for (TableName name : tableNames) {
            System.out.println(name.getNameAsString());
        }

        System.out.println("----------------------------------------");

        Table table = connection.getTable(TableName.valueOf(option.getTableName()));
        Scan scan = new Scan();
        scan.setBatch(50);
        scan.setCaching(100);
        ResultScanner scanner = table.getScanner(scan);
        Result[] results = scanner.next(100);
        for (Result result : results) {
            String rowKey = Bytes.toString(result.getRow());
            System.out.println("======row key=====>>" + rowKey);
        }

        scanner.close();
        table.close();
        connection.close();

    }
}
