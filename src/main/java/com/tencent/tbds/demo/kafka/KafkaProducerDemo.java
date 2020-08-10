package com.tencent.tbds.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

/**
 * usage:
 * java -cp dev-demo-<version>.jar:/usr/hdp/2.2.0.0-2041/kafka/libs/* com.tencent.tbds.demo.kafka.KafkaProducerDemo --auth-id <id> --auth-key <key> --kafka-brokers <kafka broker list> --topic <topic name>
 */
public class KafkaProducerDemo {
    public static void main(String[] args) throws Exception {
        KafkaDemoOption option = new KafkaDemoOption(args);
        if (option.hasHelp()) {
            option.printHelp();
            return;
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, option.getKafkaBrokers());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // TBDS原生认证参数设置
        if (option.getAuthId() == null || option.getAuthKey() == null) {
            System.out.println("authentication id and key not set");
            return;
        }
        props.put("security.protocol", "SASL_TBDS");
        props.put("sasl.mechanism", "TBDS");
        props.put("sasl.tbds.secure.id", option.getAuthId());
        props.put("sasl.tbds.secure.key", option.getAuthKey());

        // SASL/PLAIN认证参数设置
        // props.put("security.protocol", "SASL_PLAINTEXT");
        // props.put("sasl.mechanism", "PLAIN");

        String topic = option.getTopic();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter what you want to send (type quit to exit):");
        while (true) {
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("quit")) {
                break;
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            Future<RecordMetadata> future = producer.send(record);
            System.out.println(String.format("send message success: partition=%d, offset=%d", future.get().partition(), future.get().offset()));
        }

        producer.close();
    }
}
