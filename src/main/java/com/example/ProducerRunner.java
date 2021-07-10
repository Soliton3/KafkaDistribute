package com.example;


/**
 * @ Author: Xuelong Liao
 * @ Description:运行生产者的后台线程
 * @ Date: created in 14:30 2019/3/7
 * @ ModifiedBy:
 */
public class ProducerRunner {
    public static void main(String[] args) {
        boolean isAsync = args.length == 1 || !args[0].trim().equalsIgnoreCase("sync");
        String detectFile = null;
        if (args.length == 2) {
            detectFile = args[1].trim();
        }
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync, detectFile);
        producerThread.start();

    }
}
