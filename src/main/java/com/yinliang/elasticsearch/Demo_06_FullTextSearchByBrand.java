package com.yinliang.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Demo_06_FullTextSearchByBrand {

	/**
	 * 全文检索、精准查询和前缀搜索
	 * @param args
	 * @throws Exception
	 */

	/*
	数据

	PUT /car_shop/cars/5
	{
			"brand": "华晨宝马",
			"name": "宝马318",
			"price": 270000,
			"produce_date": "2017-01-20"
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


		SearchResponse searchResponse = client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.matchQuery("brand", "宝马"))
				.get();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		System.out.println("====================================================");

		searchResponse = client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.multiMatchQuery("宝马", "brand", "name"))  
				.get();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		System.out.println("====================================================");
		
		searchResponse = client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.termQuery("name.raw", "宝马318"))    
				.get();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		System.out.println("====================================================");
		
		searchResponse = client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.prefixQuery("name", "宝"))      
				.get();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		client.close();
	}
	
}
