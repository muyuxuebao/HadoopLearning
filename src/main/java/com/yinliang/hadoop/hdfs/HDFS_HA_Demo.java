package com.yinliang.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by muyux on 2015/11/24.
 */
public class HDFS_HA_Demo {
    private FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.nameservices", "ns1");
        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "itcast01:9000");
        conf.set("dfs.namenode.rpc-address.ns1.nn2", "itcast02:9000");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        fs = FileSystem.get(new URI("hdfs://ns1"), conf, "root");
    }
    /**
     * 获取块的信息
     */
    @Test
    public void testGetBlockInfo() throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("/Bugs.2014.mp4");
        FileStatus fileStatus = fs.getFileStatus(path);

        BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        System.out.println(blockLocations.length);
        for (BlockLocation blockLocation : blockLocations) {
            System.out.println(String.format("Offset:%10d, Length:%10d", blockLocation.getOffset(), blockLocation.getLength()));
            String[] hosts = blockLocation.getHosts();
            String[] names = blockLocation.getNames();
            System.out.println("hosts:");
            for (String host : hosts) {
                System.out.println(host);
            }

            System.out.println("names:");
            for (String name : names) {
                System.out.println(name);
            }
            System.out.println("===================================");

        }
    }
    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream out = fs.create(new Path("/jdk_upload.tar.gz"));
        FileInputStream in = new FileInputStream(new File("/jdk.tar.gz"));

        IOUtils.copyBytes(in, out, 4096, true);

    }

    @Test
    public void testDownload() throws IOException {
        InputStream in = fs.open(new Path("/jdk.tar.gz"));
        FileOutputStream out = new FileOutputStream(new File("D:\\jdk.tar.gz"));
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
