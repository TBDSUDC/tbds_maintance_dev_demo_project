package com.tencent.tbds.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

public class KafkaDemo {

    public static final String TOPIC = "test_topic";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/kafka/libs:/usr/hdp/2.2.0.0-2041/hadoop  -cp dev-demo-1.0-SNSHOT.jar com.tencent.tbds.demo.KafkaDemo producer|consumer");
            return;
        } else if (args[0].equals("consumer")) {
            consumerDemo();
        } else if (args[0].equals("producer")) {
            producerDemo();
        } else {
            System.out.println("usage: java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/kafka/libs:/usr/hdp/2.2.0.0-2041/hadoop  -cp dev-demo-1.0-SNSHOT.jar com.tencent.tbds.demo.KafkaDemo producer|consumer");
        }
    }

    private static void producerDemo() throws Exception {
        Properties properties = getDefaultProperties();
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        Future<RecordMetadata> future = null;

        Scanner scanner = new Scanner(System.in);
        String input;
        int sendNo = 0;
        while (true) {
            System.out.print("Enter what you want to send(exit for quit):");
            input = scanner.nextLine();
            if (input.equals("exit") || input.equals("quit")) {
                producer.close();
                return;
            }
            future = producer.send(new ProducerRecord<String, String>(TOPIC, String.valueOf(++sendNo), input));
            System.out.println("send message:[topic, offset, key, value]--->[" + TOPIC + "," + future.get().offset() + "," + sendNo + "," + input + "]");
        }

    }

    private static void consumerDemo() throws Exception {
        Properties properties = getDefaultProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: [" + record.key() + ", " + record.value() + "] at offset " + record.offset());
            }
        }
    }

    private static Properties getDefaultProperties() throws IOException {
        Properties properties = new Properties();

        // 也可以把认证以及配置信息以key-value的方式写入到一个properties配置文件中，使用时直接载入
        properties.load(new BufferedInputStream(new FileInputStream("/opt/cluster_conf/kafka/kafka_conf.properties")));

        // hard code config information
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-10-3-0-13:6667,tbds-10-3-0-4:6667,tbds-10-3-0-6:6667");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestConsumerGroup");

        //add authentication information directly
//        properties.put("security.protocol", "SASL_TBDS");
//        properties.put("sasl.mechanism", "TBDS");
//        properties.put("sasl.tbds.secure.id", "2LqMQyyITaGjQqCvmZZLnrCSWvoDSTxd4I2r");
//        properties.put("sasl.tbds.secure.key", "TwIb7Jlg643iL3twgrhZ9dCWaswok8cv");

        System.out.println("\nCheck read properties successfully:[sasl.tbds.secure.key]--->" + properties.getProperty("sasl.tbds.secure.key"));
        return properties;
    }
}
