package com.Kafka;

/**
 * Created by yinliang on 2018/7/25.
 */


public class Main {
    public static void main(String[] args) {
        KafkaProducer producerThread = new KafkaProducer(KafkaProperties.topic);
        producerThread.start();
//        KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.topic);
        KafkaLoadBalancingConsumer consumerThread = new KafkaLoadBalancingConsumer(KafkaProperties.topic);
        consumerThread.start();
    }

}
