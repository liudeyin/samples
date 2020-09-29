package com.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka消息生产者
 */
public class ProducerSample {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        //配置参数 参考ProducerConfig类
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.10.11:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for (int i=0;i<10;i++){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("testA", i + "=====这是消息哈哈哈哈哈哈哈=====");

//            // 方式1：非阻塞发送（异步）
//            produce1(kafkaProducer, record);

//            //方式2：阻塞发送（同步）
//            produce2(kafkaProducer, record);

//            //方式3：发送回调
            produce3(kafkaProducer, record);
        }
        //调用关闭资源，可以使消息全部发送
        kafkaProducer.close();

    }

    /**
     * 发送回调，不影响效率
     * 等待批量发送数据，分区不确定
     */
    private static void produce3(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> record) {
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.printf("发送回调 partition: %s,offset: %s %n", recordMetadata.partition(), recordMetadata.offset());
            }
        });
    }

    /**
     * 阻塞发送，等待结果
     */
    private static void produce2(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> record) throws Exception {
        Future<RecordMetadata> send = kafkaProducer.send(record);
        RecordMetadata recordMetadata = send.get();
        System.out.printf("阻塞发送 partition: %s,offset: %s %n", recordMetadata.partition(), recordMetadata.offset());
    }

    /**
     * 非阻塞发送，异步发送
     * 由于批量发送，分区不确定
     */
    private static void produce1(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> record) throws InterruptedException, ExecutionException {
        Future<RecordMetadata> send = kafkaProducer.send(record);
//        System.out.printf("非阻塞发送 partition: %s,offset: %s \n" + recordMetadata.partition(), recordMetadata.offset());
    }

}
