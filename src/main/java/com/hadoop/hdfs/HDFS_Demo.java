package com.hadoop.hdfs;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by muyux on 2015/11/24.
 */
public class HDFS_Demo {
    private FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(new URI("hdfs://itcast01:9000"), new Configuration(), "root");
    }

    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream out = fs.create(new Path("/HadoopLearning.jar"));
        FileInputStream in = new FileInputStream(new File("HadoopLearning.jar"));

        IOUtils.copyBytes(in, out, 4096, true);

    }

    @Test
    public void testDownload() throws IOException {
        InputStream in = fs.open(new Path("/jdk.tar.gz"));
        FileOutputStream out = new FileOutputStream(new File("jdk.tar.gz"));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    @Test
    public void testMkdir() throws IllegalArgumentException, IOException {
        boolean flag = fs.mkdirs(new Path("/itcast88888888"));
        System.out.println(flag);
    }

    @Test
    public void testDel() throws IllegalArgumentException, IOException {
        boolean flag = fs.delete(new Path("/itcast88888888"), true);
        System.out.println(flag);
    }

}
