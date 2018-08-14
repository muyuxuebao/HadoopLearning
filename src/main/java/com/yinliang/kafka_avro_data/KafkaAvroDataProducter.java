package com.yinliang.kafka_avro_data;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yinliang on 2018/8/14.
 */
public class KafkaAvroDataProducter {
    private static final String TOPIC = "TOPIC_AVRE";

    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaProducer<String, byte[]> producer = generateProducer();


        InputStream inputStream = ClassLoader.getSystemResourceAsStream("avro/user.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);


        for (int i = 0; i < 9999999; i++) {
            GenericData.Record userRecord = new GenericData.Record(schema);
            userRecord.put("name", "Str 1-" + i);
            userRecord.put("age", 20 + i);
            userRecord.put("email", "zhangsan_" + i + "@163.com");

            byte[] bytes = recordInjection.apply(userRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(TOPIC, "" + i, bytes);
            producer.send(record);
            Thread.sleep(1000);
        }

    }

    private static KafkaProducer<String, byte[]> generateProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.104.75:9093,192.168.104.75:9094,192.168.104.75:9095");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        return new KafkaProducer<String, byte[]>(props);
    }
}
