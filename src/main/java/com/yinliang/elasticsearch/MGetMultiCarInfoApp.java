package com.yinliang.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class MGetMultiCarInfoApp {
	
	@SuppressWarnings({ "resource", "unchecked" })
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
		
		for(MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
			GetResponse getResponse = multiGetItemResponse.getResponse();
			if(getResponse.isExists()) {
				System.out.println(getResponse.getSourceAsString());  
			}
		}
		
		client.close();
	}
	
}
