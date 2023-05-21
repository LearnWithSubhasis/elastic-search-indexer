package org.nextlevel.es.mapping;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.common.ConfigUtil;
import org.nextlevel.es.config.Index;
import org.nextlevel.es.config.IndexConfig;
import org.nextlevel.es.config.IndexConfigReader;
import org.nextlevel.es.tc.TransportClientProvider;

/**
 * 
 * @author nextlevel
 *
 */
public final class MappingManager {
	private static Logger logger = LoggerFactory.getLogger(MappingManager.class);
	
	private static MappingManager instance = new MappingManager();
	
	private TransportClientProvider tcp = TransportClientProvider.getInstance();
	private IndexConfig indexConfig = IndexConfigReader.getInstance().getIndexConfiguration();
	private HashMap<String, Index> indexMap = null;
	
	private MappingManager() {
		indexMap = IndexConfigReader.getInstance().getIndexMap();
	}
	
	public static MappingManager getInstance() {
		return instance;
	}
	
	public void initialize() {
		try {
			createIndex();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		//createMappingForTenantsMetadata();
		
		Set<String> indexKeys = indexMap.keySet();
		for (String indexName : indexKeys) {
			Index indx = indexMap.get(indexName);
			String indexDocType = indx.getDocumentType();
			try {
				createMappingForType(indexDocType);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}

	private void createMappingForTenantsMetadata() {
		try {
			//createMappingForType("TenantsMetadata");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	private void createIndex() throws Exception {
		String parentIndexName = indexConfig.getParentIndexName();
		
        final IndicesExistsResponse res = tcp.getTransportClient().admin().indices().prepareExists(parentIndexName).execute().actionGet();
        if (!res.isExists()) {
        	CreateIndexRequestBuilder req = tcp.getTransportClient().admin().indices().prepareCreate(parentIndexName);
        	req.setSettings(getSource("settings"));
        	CreateIndexResponse resp1 = req.execute().get();

        	String msg = new StringBuffer("Index <").append(parentIndexName).append("> created: ").append(resp1.isAcknowledged()).toString();
    		System.out.println(msg);
    		logger.info(msg);
        } else {
        	String msg = new StringBuffer("Index exists..skipped creation: ").append(parentIndexName).toString();
        	System.out.println(msg);
        	logger.info(msg);
        }		
	}
	
	private void createMappingForType(String indexDocType) throws Exception {
		Index indx = indexMap.get(indexDocType);
		if(null != indx && indx.isSkipIndexCreation())
			return;
		
		String source = indexDocType;
		boolean multipleTypes = false;
		if(null != indx && null != indx.getCreateWithParent() && indx.getCreateWithParent().isCreateWithParentMapping()) {
			source = indx.getCreateWithParent().getCombinedMappingFileName();
			multipleTypes = true;
		}
		
		String parentIndexName = indexConfig.getParentIndexName();
		
        TypesExistsRequestBuilder req = tcp.getTransportClient().admin().indices().prepareTypesExists(parentIndexName).setTypes(indexDocType);
        TypesExistsResponse resp2 = req.execute().actionGet();
        if(!resp2.isExists()) {
        	PutMappingResponse putMappingResponse = null;
        	
        	if(!multipleTypes) {
				putMappingResponse = tcp.getTransportClient().admin().indices()
				    .preparePutMapping(parentIndexName)
				    .setType(indexDocType)
				    .setSource(getSource(source))
				    .execute().actionGet();
        	} else {
        		putMappingResponse = tcp.getTransportClient().admin().indices()
    				    .preparePutMapping(parentIndexName)
    				    .setIndices(indexConfig.getParentIndexName())
    				    .setSource(getSource(source))
    				    .execute().actionGet();
        	}
			
        	String msg = new StringBuffer("Mapping <").append(indexDocType).append("> created: ").append(putMappingResponse.isAcknowledged()).toString();
    		System.out.println(msg);
    		logger.info(msg);
        } else {
        	String msg = "Mapping <"+ indexDocType +"> already exists...skipped creation.";
        	System.out.println(msg);
        	logger.info(msg);
        }
	}

	private String getSource(String documentType) throws IOException {
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		String fileNameMapping = (documentType.endsWith(".json")) ? documentType : documentType + ".json";
		InputStream configFile = contextClassLoader.getResourceAsStream(fileNameMapping);
		
		if (null == configFile) {
			ConfigUtil configUtil = ConfigUtil.getInstance();
			String configPath = configUtil.getConfigPath();
			fileNameMapping = new StringBuffer(configPath).append("/mapping/").append(fileNameMapping).toString();
			configFile = new FileInputStream(fileNameMapping);
		}
		
		StringWriter writer = new StringWriter();
		IOUtils.copy(configFile, writer, "UTF-8");
		String theString = writer.toString();		
		
		return theString;
	}	
}

