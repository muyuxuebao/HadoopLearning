package com.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by muyux on 2015/11/26.
 */
public class RPCServer implements Barty {


    public String sayHi(String name) {
        return "Hi, " + name;
    }

    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setInstance(new RPCServer())
                .setBindAddress("localhost")
                .setPort(9527)
                .setProtocol(Barty.class)
                .build();
        server.start();

    }
}
