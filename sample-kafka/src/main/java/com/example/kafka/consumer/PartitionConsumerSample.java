package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * kafka消费者指定分区消息
 * 手动提交offset
 */
public class PartitionConsumerSample {

    //主题
    static String TOPIC_NAME = "testA";

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

        //======指定消费第0个分区的内容===============
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Arrays.asList(p0));

        while (true) {
            //=====指定从第100条开始消费=====
            consumer.seek(p0, 100);
            //10S获取一次消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic: %s,partition: %s,offset: %s,value: %s %n",
                            record.topic(),record.partition(), record.offset(), record.value());
            }
            //手动提交offset到kafka
            long offset = records.records(p0).get(records.count() - 1).offset();
            Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
            offsetMap.put(p0, new OffsetAndMetadata(offset + 1));
            consumer.commitSync(offsetMap);
        }
    }
}