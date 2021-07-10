package com.example;

/**
 * @ Author: Xuelong Liao
 * @ Description:
 * @ Date: created in 19:47 2019/11/12
 * @ ModifiedBy:
 */
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ProducerPartition implements Partitioner
{

    public int partition(Object key, int numPartitions)
    {
        int partition = 0;
        int  k = (int) key;
        partition=k%numPartitions;
//        这里返回几就代表是生产者生产数据到那个分区中
        return partition;
    }
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if ((keyBytes == null) || (!(key instanceof Integer)))
            throw new InvalidRecordException("We expect all messages to have customer name as key");
        System.out.println("numPartitions: "+numPartitions+" k"+(int)key+" Partitions"+(int) key % (numPartitions ));
        // Other records will get hashed to the rest of the partitions
        return ((int) key % (numPartitions ));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
