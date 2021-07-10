package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @ Author: Xuelong Liao
 * @ Description:将读取的数据写入消费者中的文件
 * @ Date: created in 15:43 2019/3/11
 * @ ModifiedBy:
 */
public class WriteFileConsumer {
    static Properties props = new Properties();

    //poll超时时间
    private static long pollTimeout = 2000;
    private static long endTime;


    // 1.消费者配置
    static {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DetectConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //执行自定义的分配策略
//        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, DynamicAssignor.class.getName());

    }

    public static void main(String args[]) {
        PrintStream inputStream;
        try {
//            inputStream = new PrintStream(new FileOutputStream(KafkaProperties.ConsumeFile));
//            consumeAutoCommit(inputStream);
            consumeAutoCommit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 消费消息自动提交偏移值
     */
    public static void consumeAutoCommit() throws IOException {
        int flag = 1;
        long startTime=0;
        //2\. 实例化消费者
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        // 3.订阅主题
        kafkaConsumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));

        String filename1 = "/home/xiaozheng/workTest/DataSet/abnormal.txt";
        String filename2 = "/home/xiaozheng/workTest/DataSet/normal.txt";
        String filename3 = "/home/xiaozheng/workTest/DataSet/fp.txt";

        File f = new File(filename1);
        if(!f.exists()){
            f.createNewFile();
        }
        BufferedWriter bufferedWriter = null;

        try {

            // 消费者是一个长期的过程，所以使用永久循环，
//            while (true) {
                // 4.拉取消息
                ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                if (flag > 0 && !records.isEmpty()) {
                    flag--;
                    startTime=System.currentTimeMillis();
                }
                bufferedWriter = new BufferedWriter(new FileWriter(filename1));
                try{
                    for (ConsumerRecord<Integer, String> record : records) {
                        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset() + " time: " + System.currentTimeMillis());
                        if (record.value()!=null){
                            bufferedWriter.write(record.value()+ "\r\n");
                        }
                    }
                }finally {
                    try {
                        bufferedWriter.flush();
                        bufferedWriter.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
                long elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("Used Time: " + elapsedTime + " ms");
//            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}