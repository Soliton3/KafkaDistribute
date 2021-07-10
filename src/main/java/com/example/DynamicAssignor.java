package com.example;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 *
 */
public class DynamicAssignor extends AbstractPartitionAssignor {
    @Override
    public String name() {
        return "dynamic";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {
        //获取到每个topic对应的消费者列表
        Map<String, List<String>> consumersPerTopic =
                consumersPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }

        // 针对每一个topic进行分区分配
        for (Map.Entry<String, List<String>> topicEntry :
                consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue();
            int consumerSize = consumersForTopic.size();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null) {
                continue;
            }

            // 当前topic下的所有分区
            List<TopicPartition> partitions =
                    AbstractPartitionAssignor.partitions(topic,
                            numPartitionsForTopic);
            double cpu1 = 8.5;
            double cpu2 = 9.6;
            int conPartition1 = (int) (partitions.size()*(cpu1/(cpu1+cpu2)));
//            //按比例分配给消费者
//            for (int i = 0; i < conPartition1; i++) {
//                assignment.get(0).add(partitions.get(i));
//            }
//            for (int j = conPartition1; j < partitions.size(); j++) {
//                assignment.get(1).add(partitions.get(j));
//            }
//            System.out.println("总的分区数有："+partitions.size());
            // 将每个分区随机分配给一个消费者
            for (TopicPartition partition : partitions) {
                //分配分区给随机编号的消费者（0到消费者队列consumerSize最大值之间）
                int rand = new Random().nextInt(consumerSize);

                //根据编号查询到消费者
                String randomConsumer = consumersForTopic.get(rand);
                //给消费者添加分区
                assignment.get(randomConsumer).add(partition);
            }

        }
        return assignment;
    }

    // 获取每个topic所对应的消费者列表，即：[topic, List[consumer]]
    private Map<String, List<String>> consumersPerTopic(
            Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry :
                consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue().topics())
                put(res, topic, consumerId);
        }
        return res;
    }
    //获取资源预测模型中预测的cpu利用率结果
    private double getCpurate(String fileName){
        File file = new File(fileName);
        List<Double> res = new ArrayList<>();
        int num = 0;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null && num < 10) {
                //每行数据按空格隔开，第一列的1表示机器1的cpu使用率，2表示机器2的cpu使用率。
                String [] strs = tempString.split(" ");
                res.add(Double.parseDouble(strs[1]));
                num ++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        double sum = 0;
        for (double n:
             res) {
            sum += n;
        }
        return sum/num;
    }
}