package com.yinliang.kafka_avro_data;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yinliang on 2018/8/14.
 */
public class KafkaAvroDataConsumer {
    private static final String TOPIC = "TOPIC_AVRE";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.104.75:2182");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));


        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(TOPIC, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(TOPIC);
        for (final KafkaStream stream : streams) {
            ConsumerIterator it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata messageAndMetadata = it.next();
                String key = new String((byte[]) messageAndMetadata.key());
                byte[] message = (byte[]) messageAndMetadata.message();

                InputStream inputStream = ClassLoader.getSystemResourceAsStream("avro/user.avsc");
                Schema schema = new Schema.Parser().parse(inputStream);
                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                GenericRecord record = recordInjection.invert(message).get();
                System.out.println("key=" + key + ", str1= " + record.get("str1")
                        + ", str2= " + record.get("str2")
                        + ", int1=" + record.get("int1"));
            }
        }
        consumer.shutdown();
    }

}
