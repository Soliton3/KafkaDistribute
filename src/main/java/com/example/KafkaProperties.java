package com.example;

/**
 * @ Author: Xuelong Liao
 * @ Description:
 * @ Date: created in 14:31 2019/3/7
 * @ ModifiedBy:
 */
public class KafkaProperties {
    public static final String TOPIC = "detecttopic";
    public static final String KAFKA_SERVER_URL = "192.168.58.3";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";
    public static final String ConsumeFile = "/home/xiaozheng/workTest/DataSet/abnormal.txt";

    private KafkaProperties() {}
}
