package com.kafka.producer.async;

/**
 * Created by yinliang on 2018/7/31.
 */

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ASyncProduce {
    public static void main(String[] args) {
        long events = Long.MAX_VALUE;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.104.75:9093,192.168.104.75:9094,192.168.104.75:9095");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //kafka.serializer.DefaultEncoder
//        props.put("partitioner.class", "kafka.producer.partiton.SimplePartitioner");
        //kafka.producer.DefaultPartitioner: based on the hash of the key
        //props.put("request.required.acks", "1");
        props.put("producer.type", "async");
        //props.put("producer.type", "1");
        // 1: async 2: sync

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("jiketest", ip, msg);
            producer.send(data);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
        producer.close();
    }
}
