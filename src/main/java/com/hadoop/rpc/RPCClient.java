package com.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by muyux on 2015/11/26.
 */
public class RPCClient {
    public static void main(String[] args) throws IOException, NoSuchFieldException {


//        Class c=Barty.class;
//        System.out.println(c.getField("versionID"));


        Barty proxy = RPC.getProxy(Barty.class, 10010, new InetSocketAddress("localhost", 9527), new Configuration());
        String sayHi = proxy.sayHi("tomcat");
        System.out.println(sayHi);

    }
}
