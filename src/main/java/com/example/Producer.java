package com.example;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @ Author: Xuelong Liao
 * @ Description:生产者类
 * @ Date: created in 14:28 2019/3/7
 * @ ModifiedBy:
 */
public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final String fileName;
    private BufferedReader reader = null;
    private final int lineCount;

    public Producer(String topic, Boolean isAsync, String fileName) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ReadFileProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.fileName = fileName;
        lineCount  = 0;
        File file = new File(fileName);
        try {
            reader = new BufferedReader((new InputStreamReader(new FileInputStream(file))));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() {
        int messageNo = 1;
        long send_startTime = System.currentTimeMillis();
        try{
            if (reader != null) {
                String messageStr;
                long startTime = System.currentTimeMillis();
                try {
                    while ((messageStr = reader.readLine()) != null) {
                        if (isAsync) {
                            producer.send(new ProducerRecord<>(topic,
                                    messageNo,
                                    messageStr), new DemoCallBack(startTime, messageNo, messageStr));
                        } else { // Send synchronously
                            try {
                                producer.send(new ProducerRecord<>(topic,
                                        messageNo,
                                        messageStr)).get();
                                System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")" + " time:" + System.currentTimeMillis());
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                        ++messageNo;
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }finally {
            long send_endTime = System.currentTimeMillis();
            System.out.println("sending data time is:"+ (send_endTime - send_startTime) + "ms");
            try {
                reader.close();
                producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class DemoCallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
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
            System.out.println("startTime: " + startTime);
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms" + " startTime:" + startTime);
        } else {
            exception.printStackTrace();
        }
    }
}