package com.example.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * AdminClient使用示例
 */
public class AdminSample {

    public static void main(String[] args) throws Exception {

        // 获取连接
        AdminClient adminClient = adminConnect();
        // 创建队列
        createTopic();
        // 列举全部的Topic
        listTopics();

    }

    /**
     * 列举出Topic列表
     */
    public static void listTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminConnect();

        //设置展示内部使用的Topic
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> strings = listTopicsResult.names().get();

        for(String s:strings){
            System.out.println("name" + s);
        }
    }


    /**
     * 创建队列
     */
    public static void createTopic(){
        AdminClient adminClient = adminConnect();
        short rs = 2;
        NewTopic newTopic = new NewTopic("aaa", 2, rs);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
    }

    /**
     * 连接kafka
     */
    public static AdminClient adminConnect(){
        //AdminClientConfig
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.20.10.11:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        AdminClient adminClient = AdminClient.create(props);
        System.out.println("adminClient" + adminClient);
        return adminClient;
    }

}
