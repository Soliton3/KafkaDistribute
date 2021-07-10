package com.example;

/**
 * @ Author: Xuelong Liao
 * @ Description:运行消费者的线程
 * @ Date: created in 20:28 2019/3/7
 * @ ModifiedBy:
 */

public  class ConsumerRunner {
    public static void main(String[] args) {
        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, KafkaProperties.ConsumeFile);
        consumerThread.start();
    }
}