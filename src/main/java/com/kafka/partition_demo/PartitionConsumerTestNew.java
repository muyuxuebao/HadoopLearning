package com.kafka.partition_demo;

/**
 * Created by muyux on 2018/7/29.
 */

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class PartitionConsumerTestNew {
    public static void main(String args[]) {
        PartitionConsumerTestNew example = new PartitionConsumerTestNew();
        long maxReads = Long.MAX_VALUE;
        String topic = "test";

        List<InetSocketAddress> server_address = new ArrayList<InetSocketAddress>();
        server_address.add(new InetSocketAddress("192.168.83.128", 9091));
        server_address.add(new InetSocketAddress("192.168.83.128", 9092));
        server_address.add(new InetSocketAddress("192.168.83.128", 9093));

        int partLen = 3;
        for (int index = 0; index < partLen; index++) {
            try {
                example.run(maxReads, topic, index/*partition*/, server_address);
            } catch (Exception e) {
                System.out.println("Oops:" + e);
                e.printStackTrace();
            }
        }
    }


    private void run(long maxReads, String topic, int partition_index, List<InetSocketAddress> server_address) {
        // find the meta data about the topic and partition we are interested in
        //
        PartitionMetadata metadata = this.findLeader(server_address, topic, partition_index);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        System.out.println("leader info " + metadata.leader());

        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition_index;

        SimpleConsumer consumer = new SimpleConsumer( metadata.leader().host(),  metadata.leader().port(), 100000, 64 * 1024, clientName);
    }

    private PartitionMetadata findLeader(List<InetSocketAddress> server_address, String topic, int partition_index) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (int i = 0; i < server_address.size(); ++i) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(server_address.get(i).getHostName(), server_address.get(i).getPort(), 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition_index) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + server_address.get(i).getHostName() + "] to find Leader for [" + topic
                        + ", " + partition_index + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;

    }

    private List<String> m_replicaBrokers = new ArrayList<String>();
}
