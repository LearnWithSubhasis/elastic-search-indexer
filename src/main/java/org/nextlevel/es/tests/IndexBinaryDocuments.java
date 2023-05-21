package org.nextlevel.es.tests;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Base64;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.json.JSONObject;

import org.nextlevel.es.tc.TransportClientProvider;

public class IndexBinaryDocuments {
	TransportClientProvider tcp = TransportClientProvider.getInstance();
	public static void main(String[] args) {
		IndexBinaryDocuments ibd = new IndexBinaryDocuments();
		ibd.indexBinaryDoc();
	}

    private void indexBinaryDoc() {
    	Client client = null;
    	try {
			client = tcp.getTransportClient();
			IndexRequestBuilder indexRequest = client.prepareIndex("attachments", "binary");

			indexRequest.setId("att-" + new Random(System.currentTimeMillis()).nextInt(1000));
			indexRequest.setPipeline("attachment");
			
			JSONObject jsonDataObj = new JSONObject();
			jsonDataObj.put("data", getAsBase64());
			
			indexRequest.setSource(jsonDataObj.toString());
			IndexResponse resp = indexRequest.execute().actionGet();
			String result = resp.getResult().name();
			System.out.println("Result :: " + result);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(null != client) {
				client.close();
			}
		}
    	
	}

	protected String getAsBase64() throws Exception {
		String path = "publish_elastic_meetup__2_es_talk.pot.pdf";
        
        try (InputStream is = new FileInputStream(path)) {
            byte bytes[] = IOUtils.toByteArray(is);
            return Base64.getEncoder().encodeToString(bytes);
        }
    }	
}
