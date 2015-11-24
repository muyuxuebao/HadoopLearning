package com.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
public class HDFSDemo {
    private FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException {
        fs = FileSystem.get(new URI("hdfs://hadoop00:9000"), new Configuration());
    }

    @Test
    public void testDownload() {

    }

}
