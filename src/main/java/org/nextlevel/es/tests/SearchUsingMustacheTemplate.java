package org.nextlevel.es.tests;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.nextlevel.es.tc.TransportClientProvider;

public class SearchUsingMustacheTemplate {
	TransportClientProvider tcp = TransportClientProvider.getInstance();

	public static void main(String[] args) {
		SearchUsingMustacheTemplate sumt = new SearchUsingMustacheTemplate();
		sumt.search();
	}

	private void search() {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("search_term", "elasticsearch");
		
    	Client client = null;
    	try {
			client = tcp.getTransportClient();
			SearchResponse sr = new SearchTemplateRequestBuilder(client)
				    .setScript("nextlevel_search_all4")                 
				    .setScriptType(ScriptType.FILE) 
				    .setScriptParams(params)             
				    .setRequest(new SearchRequest())              
				    .get()                                        
				    .getResponse();  	
			
			SearchHits hits = sr.getHits();
			for (SearchHit searchHit : hits) {
				System.out.println(searchHit.getSourceAsString());
			}
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
	}

}
