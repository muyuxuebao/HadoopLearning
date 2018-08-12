package com.yinliang.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by muyux on 2016/1/8.
 */
public class HbaseDemoForWork {
    private Configuration conf = null;
    private String table_name = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "liang");

        table_name = "people";
    }

    @Test
    public void testCreate() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table_name));

        HColumnDescriptor hcd_info = new HColumnDescriptor("info");
        HColumnDescriptor hcd_data = new HColumnDescriptor("data");
        hcd_info.setMaxVersions(10);
        hcd_data.setMaxVersions(3);

        htd.addFamily(hcd_info);
        htd.addFamily(hcd_data);

        admin.createTable(htd);
        admin.close();
    }


    @Test
    public void testDrop() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(table_name);
        admin.deleteTable(table_name);
        admin.close();
    }

    @Test
    public void testPut() throws Exception {
        HTable table = new HTable(conf, table_name);
        Put put = new Put(Bytes.toBytes("rk0001"));

        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan1"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes("" + 1 * 100));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), 1 % 2 == 0 ? Bytes.toBytes("female") : Bytes.toBytes("male"));

        table.put(put);
        table.close();
    }

    @Test
    public void testManyPut() throws Exception {
        int maxItem = 1000;


        HTable table = new HTable(conf, table_name);

        List<Put> puts = new ArrayList<Put>(maxItem / 10);

        for (int i = 2; i <= maxItem; i++) {
            Put put = new Put(Bytes.toBytes("rk" + i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan" + i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes("" + i * 100));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), i % 2 == 0 ? Bytes.toBytes("female") : Bytes.toBytes("male"));

            puts.add(put);

            if (puts.size() > maxItem / 10) {
                table.put(puts);
                puts = new ArrayList<Put>(maxItem / 10);
            }
        }
        table.put(puts);

        table.close();
    }

    @Test
    public void testGet() throws Exception {
        //HTablePool pool = new HTablePool(conf, 10);
        //HTable table = (HTable) pool.getTable(table_name);
        HTable table = new HTable(conf, table_name);
        Get get = new Get(Bytes.toBytes("rk0001"));
        //get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.setMaxVersions(5);
        Result result = table.get(get);
        //result.getValue(family, qualifier)
        for (KeyValue kv : result.list()) {
            String family = new String(kv.getFamily());
            System.out.println(family);
            String qualifier = new String(kv.getQualifier());
            System.out.println(qualifier);
            System.out.println(new String(kv.getValue()));
        }
        table.close();
    }

    @Test
    public void testScan() throws Exception {
        HTable table = new HTable(conf, table_name);

        Scan scan = new Scan(Bytes.toBytes("rk0"), Bytes.toBytes("rk3"));  //注意，排序是按照字典顺序完成的，类似于两个字符串比较大小
        scan.addFamily(Bytes.toBytes("info"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            byte[] value = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            System.out.println(new String(value));
        }

        table.close();
    }


    @Test
    public void testDel() throws Exception {
        HTable table = new HTable(conf, table_name);
        Delete del = new Delete(Bytes.toBytes("rk0001"));
        del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
        table.delete(del);
        table.close();
    }
}