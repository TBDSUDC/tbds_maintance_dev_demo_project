***
## 工程项目说明
***
&#160; &#160; &#160; &#160;本Intelij Idea工程是针对 TBDS运维培训的第五课而开发，内容是简单的开源组件的二次开发，包含的demo程序如下：
1. HBaseDemo：简单的HBase二次开发程序，传入表名，如果不存在，则创建；然后列出所有的表
2. HDFSDemo：列出根目录/下的所有文件与文件夹
3. HiveDemo：连接hiveserver，并列出所有的数据库
4. KafkaProducerDemo：向指定topic发送数据
5. KafkaConsumerDemo：消费指定topic的数据
6. MapReduceDemo：wordCount程序
7. SparkDemo： spark版本的wordCount，使用spark-submit提交
8. SparkLauncherDemo： spark版本的wordCount，使用java直接提交
9. ReadHiveTableDemo：使用spark读取hive表的数据
10. WriteHiveTableDemo：使用spark输出数据到hive表
***
### 编译出的jar包如何运行 
&#160; &#160; &#160; &#160;使用maven编译出 dev-demo-1.0-SNAPSHOT.jar，使用java命令直接执行，各功能模块执行方式如下。

***
#### 运行 HBaseDemo 
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
```
java -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/hbase/lib/* com.tencent.tbds.demo.hbase.HBaseDemo --auth-id <id> --auth-key <key> --zk-list <zookeeper host list> --table-name <table name>
```
参数解释:  
auth-id: 认证ID  
auth-key: 认证key  
zk-list: zookeeper主机列表，如tbds-172-16-0-30,tbds-172-16-0-29    
table-name: hbase表名  

***
#### 运行 HDFSDemo 
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
```
hadoop jar dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.hdfs.HDFSDemo --auth-user <username> --auth-id <id> --auth-key <key>
```
参数解释:  
auth-user: 认证用户  
auth-id: 认证ID  
auth-key: 认证key  

***
#### 运行 HiveDemo 
**准备** <br>
代码中默认采用高可用连接方式，因此在运行程序时需要传入zk地址、用户名、密码

**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
```
java -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/hive/lib/*:/usr/hdp/2.2.0.0-2041/hadoop/hadoop-common.jar com.tencent.tbds.demo.hive.HiveDemo --zk-list <<host1:port1,host2:port2>> --user <user name> --password <password>
```

参数解释:  
zk-list: zookeeper地址列表，如tbds-172-16-0-30:2181,tbds-172-16-0-29:2181   
user: 连接hive的用户名  
password: 连接hive的密码  

***
#### 运行 KafkaProducerDemo 
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>

```
 java -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/kafka/libs/* com.tencent.tbds.demo.kafka.KafkaProducerDemo --auth-id <id> --auth-key <key> --kafka-brokers <kafka broker list> --topic <topic name>
```
参数解释:   
auth-id: 认证ID  
auth-key: 认证key  
kafka-brokers: kafka brokers列表，如tbds-172-16-0-30:6667  
topic: 指定数据发送到哪个topic

***
#### 运行 KafkaConsumerDemo
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
```
 java -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/kafka/libs/* com.tencent.tbds.demo.kafka.KafkaConsumerDemo --auth-id <id> --auth-key <key> --kafka-brokers <kafka broker list> --topic <topic name>
```
参数解释:   
auth-id: 认证ID  
auth-key: 认证key  
kafka-brokers: kafka brokers列表，如tbds-172-16-0-30:6667  
topic: 指定消费哪个topic的数据  
offset-reset: 可选参数，默认值是latest  

***
#### 运行 MapReduceDemo 
**准备** <br>
1. 在源码中做了如下设置：
    ```
     conf.addResource(new FileInputStream("/opt/cluster_conf/hadoop/core-site.xml"));
     conf.addResource(new FileInputStream("/opt/cluster_conf/hadoop/mapred-site.xml"));
    ```
    即从路径/opt/cluster_conf/hadoop/ 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
2. 在运行是要么采用export的方式完成认证，要么将认证信息配置到mapred-site.xml中</br>

**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
```
hadoop jar dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.MapReduceDemo /tmp/wordcount/input/readme.txt /tmp/wordcount/output
或
java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/hadoop:/data/bigdata/tbds/usr/hdp/2.2.0.0-2041/hadoop/share/hadoop/common/lib/ -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.MapReduceDemo /tmp/wordcount/input/readme.txt /tmp/wordcount/output
```
其中 /tmp/wordcount/input/readme.txt 为输入数据，/tmp/wordcount/output为输出目录

***
#### 运行 SparkDemo 
**准备** <br>
**运行spark程序当前仅支持export认证的方式，不支持在代码中直接注入认证信息**
1. 在运行前请export 认证信息：
    ```
     export hadoop_security_authentication_tbds_secureid=g9q06icsbwYWjQ4i2wbjz3MWNpo8DXqAZxzZ
     export hadoop_security_authentication_tbds_securekey=qbQyCiWaCJ0HmgiVpc5qofcKd8kVsJgj
     export hadoop_security_authentication_tbds_username=bhchen
    ```
    即从路径/opt/cluster_conf/hadoop/ 中读取配置信息，所以请从集群中获取该配置文件并放置到对应的路径中
    
**运行**<br>
*假定在dev-demo-1.0-SNAPSHOT.jar所在目录执行*<br>
1. 运行方式1：采用spark-submit方式
    ```
     /usr/hdp/2.2.0.0-2041/spark/bin/spark-submit --master yarn-cluster --executor-memory 3g --driver-memory 1g --num-executors 2 --executor-cores 2 --class com.tencent.tbds.demo.SparkDemo dev-demo-1.0-SNAPSHOT.jar /tmp/pyspark_demo/pyspark_demo.csv /tmp/spark_wordcount/
    ```
    其中 /tmp/wordcount/input/readme.txt 为输入数据，/tmp/wordcount/output为输出目录
2. 运行方式2：采用java的方式，该方式需要使用spark Launcher进程来把主任务调度起来：
    **原理**
    ```
    SparkAppHandle handler = new SparkLauncher()
                    .setAppName("spark-wordcount")
                    .setSparkHome("/usr/hdp/2.2.0.0-2041/spark ")
                    .setMaster("yarn")
                    .setConf("spark.driver.memory", "1g")
                    .setConf("spark.executor.memory", "3g")
                    .setConf("spark.executor.cores", "2")
                    .setConf("spark.executor.instances", "2")
                    .setAppResource("dev-demo-1.0-SNAPSHOT.jar")
                    .setMainClass("com.tencent.tbds.demo.SparkDemo")
                    .addAppArgs(args[0], args[1])
                    .setDeployMode("cluster")
    ```
   **执行**
    ```
    java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/spark/jars:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.SparkLauncherDemo /usr/hdp/2.2.0.0-2041/spark cluster /tmp/pyspark_demo/pyspark_demo.csv /tmp/spark_wordcount/
    ```
***
#### 运行 ReadHiveTableDemo
**这是一个使用spark读取hive表的示例程序**

运行步骤:
1. 使用maven命令打包项目：mvn clean compile package
2. 将target下的zip包上传到服务器
3. 解压zip包，进入解压目录
4. 执行
```
./bin/read_hive_table_demo.sh --auth-user <user name> --auth-id <secure id> --auth-key <secure key> --hive-metastore-uris <hive metastore address> --hive-db <hive database> --hive-table <hive table name>
```
参数解释:  
auth-user: 认证用户  
auth-id: 认证ID  
auth-key: 认证key  
hive-metastore-uris: hive metastore的地址  
hive-db: hive数据库  
hive-table: hive表  

***
#### 运行 WriteHiveTableDemo
**这是一个使用spark往hive表写数据的示例程序**

运行步骤:
1. 使用maven命令打包项目：mvn clean compile package
2. 将target下的zip包上传到服务器
3. 解压zip包，进入解压目录
4. 执行
```
./bin/write_hive_table_demo.sh --auth-user <user name> --auth-id <secure id> --auth-key <secure key> --hive-metastore-uris <hive metastore address> --hive-db <hive database> --hive-table <hive table name> --hdfs-path <hdfs path>
```
参数解释:  
auth-user: 认证用户  
auth-id: 认证ID  
auth-key: 认证key  
hive-metastore-uris: hive metastore的地址  
hive-db: hive数据库  
hive-table: hive表  
hdfs-path: 读取数据的路径  