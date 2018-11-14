package com.yinliang.elasticsearch;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Demo_05_SearchTemplatePageQuery {
	/**
	 * 搜索模板的功能，java api怎么去调用一个搜索模板
	 * @param args
	 * @throws Exception
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
	
		Map<String, Object> scriptParams = new HashMap<String, Object>();
		scriptParams.put("from", 0);
		scriptParams.put("size", 1);
		scriptParams.put("brand", "宝马");
		
		SearchResponse searchResponse = new SearchTemplateRequestBuilder(client)
				.setScript("page_query_by_brand")
				.setScriptType(ScriptType.FILE)
				.setScriptParams(scriptParams)
				.setRequest(new SearchRequest("car_shop").types("sales"))
				.get()
				.getResponse();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		client.close();
	}
	
}
