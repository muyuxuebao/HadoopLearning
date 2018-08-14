package com.yinliang.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.hbase.util.ClassLoaderBase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class AvroSimpleDemo {
    public static void main(String[] args) throws IOException {
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("avro/user.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);

        GenericRecord user = new GenericData.Record(schema);

        user.put("name", "张三");
        user.put("age", 30);
        user.put("email", "zhangsan@*.com");

        // 写数据
        File diskFile = new File(ClassLoader.getSystemResource("./").getPath() + "users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, diskFile);
        dataFileWriter.append(user);
        dataFileWriter.close();

        // 读数据
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(diskFile, datumReader);
        GenericRecord _current = null;
        while (dataFileReader.hasNext()) {
            _current = dataFileReader.next(_current);
            System.out.println(_current.toString());
        }

        dataFileReader.close();
    }
}
