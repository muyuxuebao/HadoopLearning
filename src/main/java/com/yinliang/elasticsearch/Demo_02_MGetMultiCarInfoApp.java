package com.yinliang.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Demo_02_MGetMultiCarInfoApp {

    /**
     * mget，一次性将多个document的数据查询出来，放在一起显示，多个汽车的型号，一次性拿出了多辆汽车的信息
     *
     * @param args
     * @throws Exception
     */

    /*

    数据
     PUT /car_shop/cars/2
    {
        "brand": "奔驰",
        "name": "奔驰C200",
        "price": 350000,
        "produce_date": "2017-01-05"
    }

     */
    @SuppressWarnings({"resource", "unchecked"})
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "yl_test-application")
                .put("client.transport.sniff", true)
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.55.170"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.55.170"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.55.170"), 9302));

        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add("car_shop", "cars", "1")
                .add("car_shop", "cars", "2")
                .get();

        for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
            GetResponse getResponse = multiGetItemResponse.getResponse();
            if (getResponse.isExists()) {
                System.out.println(getResponse.getSourceAsString());
            }
        }

        client.close();
    }

}
