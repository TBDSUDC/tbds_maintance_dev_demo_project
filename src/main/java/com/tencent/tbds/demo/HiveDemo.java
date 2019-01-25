package com.tencent.tbds.demo;

import java.sql.*;

public class HiveDemo {
    public static void main(String args[]) {
        if (args.length < 3) {
            System.out.println("usage:java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hive/lib:/usr/hdp/2.2.0.0-2041/hadoop " +
                    "-cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HiveDemo zkIP:port user userPasswd");
            System.out.println("examples: usage:java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hive/lib:/usr/hdp/2.2.0.0-2041/hadoop -cp " +
                    "dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HiveDemo tbds-10-3-0-13:2181,tbds-10-3-0-17:2181 demoUser demoUserPassword");
            return;
        }

        Connection conn;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");

//            conn = DriverManager.getConnection("jdbc:hive2://tbds-10-3-0-13:10000/default", "bhchen", "bhchen_123");

            //高可用方式：客户端字段选择可用的hiveserver
            conn = DriverManager.getConnection("jdbc:hive2://" + args[0] + "/default;" +
                            "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2",
                    args[1], args[2]);
            Statement st = conn.createStatement();
            String sqlstring = "SHOW DATABASES";
            ResultSet rs = st.executeQuery(sqlstring);
            System.out.println("Show all databases in hive:");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
