package com.tencent.tbds.demo;

import java.sql.*;

public class HiveDemo {
    public static void main(String args[]) {
        Connection conn;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");

//            conn = DriverManager.getConnection("jdbc:hive2://tbds-10-3-0-13:10000/default", "bhchen", "bhchen_123");

            //高可用方式：客户端字段选择可用的hiveserver
            conn = DriverManager.getConnection("jdbc:hive2://tbds-10-3-0-13:2181,tbds-10-3-0-17:2181,tbds-10-3-0-7:2181/default;" +
                            "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2",
                    "demoUser", "demoUserPassword");
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
