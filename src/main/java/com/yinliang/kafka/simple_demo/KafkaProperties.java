package com.yinliang.kafka.simple_demo;

/**
 * Created by yinliang on 2018/7/25.
 */
public interface KafkaProperties {
    final static String zkConnect = "192.168.104.75:2182";
    final static String groupId = "group1";
    final static String topic = "test";
    final static String kafkaServerURL = "192.168.104.75";
    final static int kafkaServerPort = 9093;
    final static int kafkaProducerBufferSize = 64 * 1024;
    final static int connectionTimeOut = 20000;
    final static int reconnectInterval = 10000;
    final static String topic2 = "topic2";
    final static String topic3 = "topic3";
    final static String clientId = "SimpleConsumerDemoClient";
}
