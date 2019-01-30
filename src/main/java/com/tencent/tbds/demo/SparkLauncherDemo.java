package com.tencent.tbds.demo;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class SparkLauncherDemo {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("usage: java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/spark/jars:/usr/hdp/2.2.0.0-2041/hadoop -cp dev-demo-1.0-SNAPSHOT.jar com.tencent.tbds.demo.SparkLauncherDemo inputFile outputDir");
            return;
        }

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
                .startApplication(new SparkAppHandle.Listener() {
                    public void stateChanged(SparkAppHandle handle) {
                        System.out.println("**********  state  changed  **********");
                    }

                    public void infoChanged(SparkAppHandle handle) {
                        System.out.println("**********  info  changed  **********");
                    }
                });


        while (!"FINISHED".equalsIgnoreCase(handler.getState().toString()) && !"FAILED".equalsIgnoreCase(handler.getState().toString())) {
            System.out.println("id    " + handler.getAppId());
            System.out.println("state " + handler.getState());

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
