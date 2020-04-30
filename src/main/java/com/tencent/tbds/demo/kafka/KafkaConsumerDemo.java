package com.tencent.tbds.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * 认证方式一：TBDS原生认证
 * usage:
 * java -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/kafka/libs/* com.tencent.tbds.demo.kafka.KafkaConsumerDemo --auth-id <id> --auth-key <key> --kafka-brokers <kafka broker list> --topic <topic name>
 *
 * 认证方式二：SASL/PLAIN认证
 * usage:
 * java -Djava.security.auth.login.config=/etc/path/kafka_client_jaas.conf -cp dev-demo-1.0-SNAPSHOT.jar:/usr/hdp/2.2.0.0-2041/kafka/libs/* com.tencent.tbds.demo.kafka.KafkaConsumerDemo --kafka-brokers <kafka broker list> --topic <topic name>
 *
 * SASL/PLAIN认证设置：
 * 1. 修改kafka的模板文件：
 * /var/lib/tbds-server/resources/common-services/KAFKA/0.10.0.1/package/templates/kafka_jaas.conf.j2
 * 2. 添加认证用户
 * 3. 重启tbds-server服务
 * 4. 重启kafka
 * 5. 查看kafka的最终生效的配置文件/etc/kafka/conf/kafka_jaas.conf
 *
 * kafka_client_jaas.conf内容如下：
 * KafkaClient {
 *        org.apache.kafka.common.security.plain.PlainLoginModule required
 *        username="tbds user"
 *        password="password";
 * };
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        KafkaDemoOption option = new KafkaDemoOption(args);
        if (option.hasHelp()) {
            option.printHelp();
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, option.getKafkaBrokers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_001");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, option.getOffsetReset());
        // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("Received message: offset=%d, value=%s", record.offset(), record.value()));
            }
        }
    }
}
