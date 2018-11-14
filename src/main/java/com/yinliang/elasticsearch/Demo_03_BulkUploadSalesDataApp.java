package com.yinliang.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Demo_03_BulkUploadSalesDataApp {
	/**
	 * 基于bulk的API
	 * 业务场景：有一个汽车销售公司，拥有很多家4S店，这些4S店的数据，都会在一段时间内陆续传递过来，汽车的销售数据，
	 * 现在希望能够在内存中缓存比如1000条销售数据，然后一次性批量上传到es中去
	 * @param args
	 * @throws Exception
	 */

	/*
	数据
	PUT /car_shop/sales/1
	{
		"brand": "宝马",
		"name": "宝马320",
		"price": 320000,
		"produce_date": "2017-01-01",
		"sale_price": 300000,
		"sale_date": "2017-01-21"
	}

	PUT /car_shop/sales/2
	{
		"brand": "宝马",
		"name": "宝马320",
		"price": 320000,
		"produce_date": "2017-01-01",
		"sale_price": 300000,
		"sale_date": "2017-01-21"
	}

	 */
	
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
	
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop", "sales", "3") 
				.setSource(XContentFactory.jsonBuilder()
							.startObject()
								.field("brand", "奔驰")
								.field("name", "奔驰C200")
								.field("price", 350000)
								.field("produce_date", "2017-01-20")
								.field("sale_price", 320000)
								.field("sale_date", "2017-01-25")
							.endObject());
		bulkRequestBuilder.add(indexRequestBuilder);
		
		UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop", "sales", "1")
				.setDoc(XContentFactory.jsonBuilder()
						.startObject()
							.field("sale_price", 290000)
						.endObject());
		bulkRequestBuilder.add(updateRequestBuilder);
		
		DeleteRequestBuilder deleteReqeustBuilder = client.prepareDelete("car_shop", "sales", "2"); 
		bulkRequestBuilder.add(deleteReqeustBuilder);
		
		BulkResponse bulkResponse = bulkRequestBuilder.get();
		
		for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
			System.out.println("version: " + bulkItemResponse.getVersion()); 
		}
		
		client.close();
	}
	
}
