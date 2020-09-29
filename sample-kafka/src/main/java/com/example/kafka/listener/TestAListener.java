package com.example.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * kafka消息监听器
 */
@Component
public class TestAListener {

    @KafkaListener(topics = "testA")
    public void onMessage(ConsumerRecord<String, String> message){
        //insertIntoDb(buffer);//这里为插入数据库代码
        System.out.printf("topic: %s,partition: %s,offset: %s,value: %s %n",
                message.topic(),message.partition(), message.offset(), message.value());
    }


}
