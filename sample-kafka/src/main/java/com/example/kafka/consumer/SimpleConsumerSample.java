package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * kafka消费者
 * 手动提交offset
 */
public class SimpleConsumerSample {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.10.11:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //配置手动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("testA"));

        while (true) {
            //10S获取一次消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            //获取到多个分区的数据
            for (TopicPartition partition : records.partitions()) {
                //获取到每个分区中的数据
                List<ConsumerRecord<String, String>> list = records.records(partition);
                for (ConsumerRecord<String, String> record : list) {
                    System.out.printf("topic: %s,partition: %s,offset: %s,value: %s %n",
                            record.topic(),record.partition(), record.offset(), record.value());
                }
                //手动提交offset到kafka
                long offset = list.get(list.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                offsetMap.put(partition, new OffsetAndMetadata(offset + 1));
                consumer.commitSync(offsetMap);
            }
        }
    }
}
