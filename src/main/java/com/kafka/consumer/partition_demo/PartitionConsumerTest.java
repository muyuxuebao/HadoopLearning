package com.kafka.consumer.partition_demo;

/**
 * Created by muyux on 2018/7/29.
 */

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class PartitionConsumerTest {
    public static void main(String args[]) {
        PartitionConsumerTest example = new PartitionConsumerTest();
        long maxReads = Long.MAX_VALUE;
        String topic = "test";

        List<InetSocketAddress> server_address = new ArrayList<InetSocketAddress>();
        server_address.add(new InetSocketAddress("192.168.104.75", 9093));
        server_address.add(new InetSocketAddress("192.168.104.75", 9094));
        server_address.add(new InetSocketAddress("192.168.104.75", 9095));

        int partLen = 3;
        for (int index = 0; index < 3; index++) {
            try {
                example.run(maxReads, topic, index/*partition*/, server_address);
            } catch (Exception e) {
                System.out.println("Oops:" + e);
                e.printStackTrace();
            }
        }
    }


    private void run(long maxReads, String topic, int partition_index, List<InetSocketAddress> server_address) throws Exception {
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
        System.out.println("leader info " + metadata.leader() + " partition_index: " + partition_index);

        String clientName = "Client_" + topic + "_" + partition_index;

        SimpleConsumer consumer = new SimpleConsumer(metadata.leader().host(), metadata.leader().port(), 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer, topic, partition_index, kafka.api.OffsetRequest.EarliestTime(), clientName);
        System.out.println("readOffset:" + readOffset);

        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                .addFetch(topic, partition_index, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to kafka
                .build();
        FetchResponse fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError()) {
            System.out.println("has error");
        }
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition_index)) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                continue;
            }
            readOffset = messageAndOffset.nextOffset();
            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
        }

    }

    private PartitionMetadata findNewLeader(PartitionMetadata old_metadata, List<InetSocketAddress> server_address, String topic, int partition_index) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(server_address, topic, partition_index);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (old_metadata.leader().host().equals(metadata.leader().host()) && old_metadata.leader().port() == metadata.leader().port() && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
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
        return returnMetaData;

    }

}
