***
## 工程说明
***
&#160; &#160; &#160; &#160;本Intelij Idea工程是针对 TBDS运维培训的第五课而开发，内容是简单的开源组件的二次开发，包含4个类：
1. HBaseDemo：简单的HBase二次开发程序，传入表名，如果不存在，则创建；然后列出所有的表
2. HDFSDemo：列出根目录/下的所有文件与文件夹
3. HiveDemo：连接hiveserver，并列出所有的数据库
4. KafkaDemo：包含一个producer和consumer程序，一个用来向test_topic发送数据，一个用来从test_topic读取数据
5. MapReduceDemo：wordCount程序
***
### 编译出的jar包如何运行 
&#160; &#160; &#160; &#160;使用maven编译出 dev-demo-1.0-SNAPSHOT.jar，使用java命令直接执行，各功能模块执行方式如下。

***
#### 运行 HBaseDemo 
**准备** <br>
1. 在源码中做了如下设置：
    ```java
    hbaseConf.addResource(new FileInputStream("/opt/cluster_conf/hbase/hbase-site.xml"));
    ```
    即从路径/opt/cluster_conf/hbase/hbase-site.xml 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
2. 在运行是要么采用export的方式完成认证，要么将认证信息配置到hbase-site.xml中</br>

**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
```java
java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hbase/lib/:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HBaseDemo test_table
```
其中 test_table 为表名

***
#### 运行 HDFSDemo 
**准备** <br>
1. 在源码中做了如下设置：
    ```java
    hbaseConf.addResource(new FileInputStream("/opt/cluster_conf/hbase/hbase-site.xml"));
    ```
    即从路径/opt/cluster_conf/hbase/hbase-site.xml 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
2. 在运行是要么采用export的方式完成认证，要么将认证信息配置到hbase-site.xml中</br>
**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar*所在目录执行<br>
```java
java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hbase/lib/:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HBaseDemo test_table
```
其中 test_table 为表名

***
#### 运行 HiveDemo 
**准备** <br>
1. 在源码中做了如下设置：
    ```java
    hbaseConf.addResource(new FileInputStream("/opt/cluster_conf/hbase/hbase-site.xml"));
    ```
    即从路径/opt/cluster_conf/hbase/hbase-site.xml 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
2. 在运行是要么采用export的方式完成认证，要么将认证信息配置到hbase-site.xml中
<br>
**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar*所在目录执行<br>
```java
java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hbase/lib/:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HBaseDemo test_table
```
其中 test_table 为表名

***
#### 运行 KafkaDemo 
**准备** <br>
1. 在源码中做了如下设置：
    ```java
    hbaseConf.addResource(new FileInputStream("/opt/cluster_conf/hbase/hbase-site.xml"));
    ```
    即从路径/opt/cluster_conf/hbase/hbase-site.xml 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
2. 在运行是要么采用export的方式完成认证，要么将认证信息配置到hbase-site.xml中
<br>
**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar*所在目录执行<br>
```java
java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hbase/lib/:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HBaseDemo test_table
```
其中 test_table 为表名

***
#### 运行 MapReduceDemo 
**准备** <br>
1. 在源码中做了如下设置：
    ```java
    hbaseConf.addResource(new FileInputStream("/opt/cluster_conf/hbase/hbase-site.xml"));
    ```
    即从路径/opt/cluster_conf/hbase/hbase-site.xml 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
2. 在运行是要么采用export的方式完成认证，要么将认证信息配置到hbase-site.xml中
<br>
**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar*所在目录执行<br>
```java
java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hbase/lib/:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.HBaseDemo test_table
```
其中 test_table 为表名