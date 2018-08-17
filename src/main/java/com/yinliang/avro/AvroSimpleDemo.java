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
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.util.ClassLoaderBase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class AvroSimpleDemo {
    private static void test_use_compiled_class() throws IOException {
        // 使用编译好的类
        User.Builder builder = User.newBuilder();
        builder.setName("李四");
        builder.setAge(30);
        builder.setEmail("zhangsan@*.com");
        User user = builder.build();

//序列化
        File diskFile = new File(ClassLoader.getSystemResource("./").getPath() + "users.avro");
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
//指定schema
        dataFileWriter.create(User.getClassSchema(), diskFile);
        dataFileWriter.append(user);
        dataFileWriter.flush(); //多次写入之后，可以调用fsync将数据同步写入磁盘(IO)通道
        user.setName("李四");
        user.setEmail("lisi@*.com");
        dataFileWriter.append(user);
        dataFileWriter.close();

//反序列化
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
// 也可以使用DataFileStream
// DataFileStream<User> dataFileStream = new DataFileStream<User>(new FileInputStream(diskFile),userDatumReader);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(diskFile, userDatumReader);
        User _current = null;
        while (dataFileReader.hasNext()) {
            //注意:avro为了提升性能，_current对象只会被创建一次，且每次遍历都会重用此对象
            //next方法只是给_current对象的各个属性赋值，而不是重新new。
            _current = dataFileReader.next(_current);
            //toString方法被重写，将获得JSON格式
            System.out.println(_current);
        }
        dataFileReader.close();


    }

    private static void test_direct_use_schema_file() throws IOException {
        // 直接使用schema文件
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


    public static void main(String[] args) throws IOException {
        test_direct_use_schema_file();
        test_use_compiled_class();
    }


}
