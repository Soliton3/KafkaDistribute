package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @ Author: Xuelong Liao
 * @ Description:消费者类
 * @ Date: created in 20:27 2019/3/7
 * @ ModifiedBy:
 */
public class Consumer extends Thread {

    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private String filePath;
    private PrintStream inputStream;
    public Consumer(String topic, String filePath) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DetectConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //执行自定义的分配策略
//        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, DynamicAssignor.class.getName());

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.filePath = filePath;
        try {
            inputStream = new PrintStream(new FileOutputStream(this.filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            inputStream.append(record.value()+"\r\n");
        }

    }
}
