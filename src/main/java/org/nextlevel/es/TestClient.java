package org.nextlevel.es;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.common.bytes.BytesReference;

import org.nextlevel.es.tc.TransportClientProvider;

/**
 * 
 * @author nextlevel
 *
 */
public class TestClient {
	public static void main(String[] args) {
		TestClient tc = new TestClient();
		try {
			//tc.test();
			tc.test2();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void test() throws Exception {
		TransportClientProvider tcp = TransportClientProvider.getInstance();
		String indexName = "test-index";
		
        final IndicesExistsResponse res = tcp.getTransportClient().admin().indices().prepareExists(indexName).execute().actionGet();
        if (!res.isExists()) {
    		CreateIndexResponse resp1 = tcp.getTransportClient().admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
    		System.out.println("Index created: " + resp1.isAcknowledged());
        } else {
        	System.out.println("Index exists..skipped creation.");
        }

        String mappingName = "contact";
        
        TypesExistsRequestBuilder req = tcp.getTransportClient().admin().indices().prepareTypesExists(indexName).setTypes(mappingName);
        TypesExistsResponse resp2 = req.execute().actionGet();
        if(!resp2.isExists()) {
			PutMappingResponse putMappingResponse = tcp.getTransportClient().admin().indices()
			    .preparePutMapping(indexName)
			    .setType(mappingName)
			    .setSource(getSource(mappingName))
			    .execute().actionGet();
			
			System.out.println("Mapping created: " + putMappingResponse.isAcknowledged());
        } else {
        	System.out.println("Mapping already exists.");
        }

        mappingName = "kbArticle";
        
        req = tcp.getTransportClient().admin().indices().prepareTypesExists(indexName).setTypes(mappingName);
        resp2 = req.execute().actionGet();
        if(!resp2.isExists()) {
			PutMappingResponse putMappingResponse = tcp.getTransportClient().admin().indices()
			    .preparePutMapping(indexName)
			    .setType(mappingName)
			    .setSource(getSource(mappingName))
			    .execute().actionGet();
			
			System.out.println("Mapping created: " + putMappingResponse.isAcknowledged());
        } else {
        	System.out.println("Mapping already exists.");
        }
        
        
//		IndexResponse response1 = tcp.getTransportClient().prepareIndex("test-index", "indextype1")
//		    .setSource(buildIndex())
//		    .execute()
//		    .actionGet(); 
	}


	public void test2() throws Exception {
		TransportClientProvider tcp = TransportClientProvider.getInstance();
		String indexName = "test-index";
		
        final IndicesExistsResponse res = tcp.getTransportClient().admin().indices().prepareExists(indexName).execute().actionGet();
        if (!res.isExists()) {
//    		CreateIndexResponse resp1 = tcp.getTransportClient().admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
        	CreateIndexRequestBuilder req = tcp.getTransportClient().admin().indices().prepareCreate(indexName);
        	req.setSettings(getSource("settings"));
        	CreateIndexResponse resp1 = req.execute().get();
    		System.out.println("Index created: " + resp1.isAcknowledged());
        } else {
        	System.out.println("Index exists..skipped creation.");
        }

        String mappingName = "contact";
        
        TypesExistsRequestBuilder req = tcp.getTransportClient().admin().indices().prepareTypesExists(indexName).setTypes(mappingName);
        TypesExistsResponse resp2 = req.execute().actionGet();
        if(!resp2.isExists()) {
			PutMappingResponse putMappingResponse = tcp.getTransportClient().admin().indices()
			    .preparePutMapping(indexName)
			    .setType(mappingName)
			    .setSource(getSource(mappingName))
			    .execute().actionGet();
			
			System.out.println("Mapping created: " + putMappingResponse.isAcknowledged());
        } else {
        	System.out.println("Mapping already exists.");
        }

        mappingName = "kbArticle";
        
        req = tcp.getTransportClient().admin().indices().prepareTypesExists(indexName).setTypes(mappingName);
        resp2 = req.execute().actionGet();
        if(!resp2.isExists()) {
			PutMappingResponse putMappingResponse = tcp.getTransportClient().admin().indices()
			    .preparePutMapping(indexName)
			    .setType(mappingName)
			    .setSource(getSource(mappingName))
			    .execute().actionGet();
			
			System.out.println("Mapping created: " + putMappingResponse.isAcknowledged());
        } else {
        	System.out.println("Mapping already exists.");
        }
        
        
//		IndexResponse response1 = tcp.getTransportClient().prepareIndex("test-index", "indextype1")
//		    .setSource(buildIndex())
//		    .execute()
//		    .actionGet(); 
	}
	
	private String getSource(String documentType) throws IOException {
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		InputStream configFile = contextClassLoader.getResourceAsStream(documentType + ".json");				
		StringWriter writer = new StringWriter();
		IOUtils.copy(configFile, writer, "UTF-8");
		String theString = writer.toString();		
		
		return theString;
	}

	private BytesReference buildIndex() {
		return null;
	}
}
