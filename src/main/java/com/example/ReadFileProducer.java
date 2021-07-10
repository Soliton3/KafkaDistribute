package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * @ Author: Xuelong Liao
 * @ Description:从生产者的文件中读取数据进行上传
 * @ Date: created in 11:13 2019/3/11
 * @ ModifiedBy:
 */
public class ReadFileProducer {

    public static void main(String[] args) {
        String detectFile="";
        if (args.length == 1) {
            detectFile = args[0].trim();
        }
        KafkaProducer<Integer, String> producer;
        Properties props = new Properties();
        String topic = KafkaProperties.TOPIC;
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ReadFileProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
        File file = new File(detectFile);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            long startTime = System.currentTimeMillis();
            int messageNo = 1;
            String messageStr;
            while ((messageStr = reader.readLine()) != null) {
                //同步上传
//                producer.send(new ProducerRecord<>(topic,
//                        messageNo, messageStr)).get();
                producer.send(new ProducerRecord<>(topic,
                        messageNo, messageStr), new CallBack(startTime, messageNo, messageStr));
                long elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("Sent message: (" + messageNo + ", " + messageStr + ") in " + elapsedTime + " ms, " +"startTime: " + startTime);
                ++messageNo;
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}

class CallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;

    public CallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms" + " startTime:" + startTime);
        } else {
            exception.printStackTrace();
        }
    }
}
