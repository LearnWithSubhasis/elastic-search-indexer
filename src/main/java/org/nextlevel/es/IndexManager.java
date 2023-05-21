package org.nextlevel.es;

import java.util.ArrayList;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.config.Index;
import org.nextlevel.es.config.IndexConfig;
import org.nextlevel.es.config.IndexConfigReader;
import org.nextlevel.es.config.IndexUtil;
import org.nextlevel.es.db.util.DatabaseUtil;
import org.nextlevel.es.mapping.MappingManager;
import org.nextlevel.es.tc.TransportClientProvider;
import org.nextlevel.es.tenants.TenantsConfiguration;
import org.nextlevel.es.tenants.TenantsConfigurationReader;

/**
 * 
 * @author nextlevel
 *
 */
public abstract class IndexManager {
	
	//private static final int ES_RESULT_SIZE = 999999999;
	private static final int ES_RESULT_SIZE = 10;

	private static Logger logger = LoggerFactory.getLogger(IndexClient.class);
	protected TransportClientProvider tcp = TransportClientProvider.getInstance();
	protected Client esClient = null;
	protected IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
	protected TenantsConfiguration tenantsConfigData = TenantsConfigurationReader.getInstance().getConfiguration();
	protected DatabaseUtil dbUtil = new DatabaseUtil();
	
	protected IndexOperationMethod indexOperationMethod;
	protected String tenantID = null;
	protected String indexName; //Basically index alias name (slice id)
	protected String indexDocumentType; 
	protected ArrayList<Long> selectedIDsToIndex;
	protected IndexUtil indexUtil = new IndexUtil();
	
	private String configDir;
		
	public IndexOperationMethod getIndexOperationMethod() {
		return indexOperationMethod;
	}

	public void setIndexOperationMethod(IndexOperationMethod indexOperationMethod) {
		this.indexOperationMethod = indexOperationMethod;
	}

	public ArrayList<Long> getSelectedIDsToIndex() {
		return selectedIDsToIndex;
	}

	public void setSelectedIDsToIndex(ArrayList<Long> selectedIDsToIndex) {
		this.selectedIDsToIndex = selectedIDsToIndex;
	}

	public String getTenantID() {
		return tenantID;
	}

	public void setTenantID(String tenantID) {
		this.tenantID = tenantID;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	
	public abstract boolean index();
	
	public abstract boolean indexNearRealTime();
	
	/**
	 * Initializes index and mappings - if they don't exist already
	 */
	protected void initializeIndexAndMappings() {
		MappingManager mm = MappingManager.getInstance();
		mm.initialize();
	}
	
	/**
	 * 
	 * @param indexName - this in our case will be the alias name (slice id)
	 * @return
	 */
	protected boolean isExists(String indexName) {
		final AliasesExistResponse resp = esClient.admin().indices().prepareAliasesExist(indexName).execute().actionGet();
		return resp.isExists();
	}
	
	
	
	/**
	 * 
	 * @param indexName - this in our case will be the alias name (slice id)
	 * @return
	 */
	protected boolean deleteIndex(String indexName) {
		if(isExists(indexName)) {
			final DeleteIndexRequestBuilder delIdx = esClient.admin().indices().prepareDelete(indexName);
	        delIdx.execute().actionGet();
	        return !isExists(indexName);
		}		

		return false;
	}

	protected void indexData(Index index, JSONObject jsonDataObj, String sliceID) {
		indexData(index, jsonDataObj, sliceID, getIDKey(index));
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 */
	protected String getIDKey(Index index) {
		if(null != index && null != index.getIDKey()) {
			return index.getIDKey();
		}
		return "row_id";
	}

	/**
	 * 
	 * @param index - Index object
	 * @param jsonDataObj - json object to index
	 * @param sliceID 
	 * @param idKey - DB column that will be mapped to _id attribute (ElasticSearch document). 
	 */
	protected boolean indexData(Index index, JSONObject jsonDataObj, String sliceID, String idKey) {
		String parentIndexName = indexConfigReader.getParentIndexName(index);
		if(null == idKey || idKey.trim().length()==0) {
			idKey = "row_id";
		}
		
		switch(indexOperationMethod) {
		case Create:
		case Update:
			createUpdateIndexData(index, parentIndexName, jsonDataObj, idKey);
			break;
			
		case Delete:
			deleteIndexData(index, parentIndexName, sliceID, idKey);
			break;
			
		}
		
		return true;
	}
	
	private void deleteIndexData(Index index, String parentIndexName,
			String sliceID, String idKey) {
		if(getSelectedIDsToIndex().size() == 0) {
			deleteIndexBySlice(parentIndexName, index.getDocumentType(), sliceID, true);
		} else {
			deleteIndexBySliceAndID(parentIndexName, index.getDocumentType(), sliceID);
		}
	}

	private void createUpdateIndexData(Index index, String parentIndexName, JSONObject jsonDataObj, String idKey) {
		IndexRequestBuilder indexRequest = esClient.prepareIndex(parentIndexName, index.getName());

		System.out.println("Fetching ID value...");
		logger.info("Fetching ID value...");
		String idValue = getIDValue(jsonDataObj, idKey, index);
		if(null != idValue) {
			System.out.println("Final call to index record (ElasticSearch):: ID: " + idValue);
			logger.info("Final call to index record (ElasticSearch):: ID: " + idValue);
			
			if(index.getName().equalsIgnoreCase("conversation")) {
				String tenant = jsonDataObj.getString("tenant");
				int projectID = (int) jsonDataObj.get("resource_id");
				String projectKey = new StringBuffer(tenant).append("_").append("project").append("_").append(projectID).toString();
				//jsonDataObj.put("parent", projectKey);
				indexRequest.setParent(projectKey);
			}
			
			indexRequest.setId(idValue);
			indexRequest.setSource(jsonDataObj.toString());
			IndexResponse resp = indexRequest.execute().actionGet();
//			if (resp.isCreated()) {
////			    if(logger.isDebugEnabled()) {
////			    	logger.debug("Document created: " + jsonDataObj.toString());
////			    }
//				logger.info("Document created: " + jsonDataObj.toString());
//			} else {
////			    if(logger.isDebugEnabled()) {
////			    	logger.debug("Document updated: " + jsonDataObj.toString());
////			    }
//				logger.info("Document updated: " + jsonDataObj.toString());
//			}		
		} else {
			logger.error("Invalid value for id column <" + index.getIDKey() + ">.");
			logger.error("Data: " + jsonDataObj);
		}
	}
	
	protected String getIDValue(JSONObject jsonDataObj, String idKey, Index index) {
		StringBuilder idValue = new StringBuilder();
		//idValue.append(jsonDataObj.getString("tenant")).append("_").append(index.getDocumentType()).append("_");
		try {
			//idValue = String.valueOf(jsonDataObj.getInt(idKey));
			idValue.append(jsonDataObj.getInt(idKey));
			logger.info("Found id value: " + idValue + ", for id key: " + idKey);
		} catch (Exception ex) {
			if(idKey.indexOf(".")>=0) {
				idKey = idKey.substring(idKey.indexOf(".")+1);
			}
			
			try {
				//idValue = String.valueOf(jsonDataObj.getInt(idKey));
				idValue.append(jsonDataObj.getInt(idKey));
				logger.info("Found id value: " + idValue.toString() + ", for altered id key: " + idKey);
			} catch (Exception ex2) {
				logger.error("Cannot get the value for row_id column...");
			}
		}
		
		logger.info("Final id value: " + idValue.toString());
		return idValue.toString();
	}	
	
	public String getIndexDocumentType() {
		return indexDocumentType;
	}

	public void setIndexDocumentType(String indexDocumentType) {
		this.indexDocumentType = indexDocumentType;
	}

	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}
	
	protected boolean bulkIndex(JSONArray jsonArray, Index index) {
		logger.info("Initiated bulk index for: " + index.getName());
		
		BulkRequestBuilder bulkRequest = esClient.prepareBulk();
		String parentIndexName = indexConfigReader.getParentIndexName(index);
		
		int counter = 0;
		int currentBatchNo = 1;
		boolean bReturn = true;
		
		int totalBatches = (jsonArray.length()/indexConfigReader.getBatchCount()) + 1;
		String msg = new StringBuffer("======== Index: ").append(index.getDocumentType()).append(", Total records: " ).append( jsonArray.length() ).append( ", Per batch: " ).append( indexConfigReader.getBatchCount() ).append( ", Total batches: " ).append( totalBatches).toString();
		System.out.println(msg);
		logger.info(msg);
		
		for(int i=0; i<jsonArray.length(); i++, counter++) {
			JSONObject jsonDataObj = (JSONObject) jsonArray.get(i);
			
			String idVal = getIDValue(jsonDataObj, index.getIDKey(), index);
			String projectKey = null;
			if(index.getName().equalsIgnoreCase("conversation")) {
				String tenant = jsonDataObj.getString("tenant");
				long projectID = (long) jsonDataObj.get("resource_id");
				projectKey = new StringBuffer(tenant).append("_").append("project").append("_").append(projectID).toString();
				//jsonDataObj.put("parent", projectKey);
			}
			
			if(null != idVal && null != projectKey) {
				// either use client#prepare, or use Requests# to directly build index/delete requests
				bulkRequest.add(esClient.prepareIndex(parentIndexName, index.getName(), idVal)
				        .setSource(jsonDataObj.toString())
				        .setParent(projectKey)
				        );
				if(logger.isDebugEnabled()) {
					logger.debug("Added for Bulk Index: " + index.getName() + ", Data: "+ jsonDataObj.toString());
				} 
			} else if(null != idVal) {
				// either use client#prepare, or use Requests# to directly build index/delete requests
				bulkRequest.add(esClient.prepareIndex(parentIndexName, index.getName(), idVal)
				        .setSource(jsonDataObj.toString())
				        );
				if(logger.isDebugEnabled()) {
					logger.debug("Added for Bulk Index: " + index.getName() + ", Data: "+ jsonDataObj.toString());
				} 
			}
			
			if(counter >= indexConfigReader.getBatchCount() || i>=jsonArray.length()-1) {
				msg = new StringBuffer("======== Current Batch Number: " ).append( currentBatchNo++).append( " / ").append(totalBatches).append(" =========").toString();
				System.out.println(msg);
				logger.info(msg);
				
				//Reset counter for next batch
				counter = 0;
				
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					bReturn = false;
					logger.error(bulkResponse.buildFailureMessage());
				}	
				
				TimeValue timeTaken = bulkResponse.getTook();
				msg = new StringBuffer("Time taken (Seconds): ").append(timeTaken.getSeconds()).append(", Per Document (ms): ").append(timeTaken.getMillis()/indexConfigReader.getBatchCount())  
						.toString();
				System.out.println(msg);
				logger.info(msg);
			}			
		}		
		return bReturn;
	}
	
	public void createAlias(int sliceID, Index index) {
		try {
			IndicesAdminClient indicesAdminClient = esClient.admin().indices();
			AliasesExistResponse aliasExistsResp = indicesAdminClient.prepareAliasesExist(String.valueOf(sliceID)).get();
			if(!aliasExistsResp.exists()) {
				IndicesAliasesResponse aliasCreateResp = indicesAdminClient.prepareAliases().addAlias(index.getParentIndexName(), String.valueOf(sliceID)).get();
				if(aliasCreateResp.isAcknowledged()) {
					String msg = "Alias <"+sliceID+"> for index <" + index.getParentIndexName() + "> is created. Please note that alias can be created on index and not on document types.";
					logger.info(msg);
					System.out.println(msg);
				} else {
					String msg = "Alias <"+sliceID+"> for index <" + index.getParentIndexName() + "> is NOT created. Please check if the request was sent properly.";
					logger.warn(msg);
					System.out.println(msg);
				}
			} else {
				String msg = "Alias <"+sliceID+"> for index <" + index.getParentIndexName() + "> already exists. Skipped alias creation.";
				logger.info(msg);
				System.out.println(msg);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		}		
	}
	
	public void createAlias(String sliceID, String parentIndexName) {
		try {
			IndicesAdminClient indicesAdminClient = esClient.admin().indices();
			AliasesExistResponse aliasExistsResp = indicesAdminClient.prepareAliasesExist(sliceID).get();
			if(!aliasExistsResp.exists()) {
				IndicesAliasesResponse aliasCreateResp = indicesAdminClient.prepareAliases().
						addAlias(parentIndexName, String.valueOf(sliceID),
								new TermQueryBuilder("tenant", sliceID)).get();
				if(aliasCreateResp.isAcknowledged()) {
					String msg = "Alias <"+sliceID+"> for index <" + parentIndexName + "> is created. Please note that alias can be created on index and not on document types.";
					logger.info(msg);
					System.out.println(msg);
				} else {
					String msg = "Alias <"+sliceID+"> for index <" + parentIndexName + "> is NOT created. Please check if the request was sent properly.";
					logger.warn(msg);
					System.out.println(msg);
				}
			} else {
				String msg = "Alias <"+sliceID+"> for index <" + parentIndexName + "> already exists. Skipped alias creation.";
				logger.info(msg);
				System.out.println(msg);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		}
	}	
	
	public void deleteAlias(String parentIndexName, String typeName, int sliceID) {
		try {
			IndicesAdminClient indicesAdminClient = esClient.admin().indices();
			AliasesExistResponse aliasExistsResp = indicesAdminClient.prepareAliasesExist(String.valueOf(sliceID)).get();
			if(aliasExistsResp.exists()) {
				IndicesAliasesResponse aliasDeleteResp = indicesAdminClient.prepareAliases()
						.removeAlias(parentIndexName, String.valueOf(sliceID))
						.get();
				if(aliasDeleteResp.isAcknowledged()) {
					String msg = "Alias <"+sliceID+"> for index <" + parentIndexName + "> is deleted.";
					logger.info(msg);
					System.out.println(msg);
				} else {
					String msg = "Alias <"+sliceID+"> for index <" + parentIndexName + "> is NOT deleted. Please check if the request was sent properly.";
					logger.warn(msg);
					System.out.println(msg);
				}
			} else {
				String msg = "Alias <"+sliceID+"> for index <" + parentIndexName + "> does not exist. Skipped alias deletion.";
				logger.info(msg);
				System.out.println(msg);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		}
	}
	
//	public void deleteIndexBySlice(String indexName, String typeName, int sliceID) {
//        DeleteByQueryRequestBuilder delete = newDeleteByQuery()
//        		//.setIndices(String.valueOf(sliceID))
//        		.setIndices(indexName)
//        		.setTypes(typeName)
//        		.setQuery(QueryBuilders.matchQuery("slice", Integer.toString(sliceID)));
//        
//        DeleteByQueryResponse delResp = delete.execute().actionGet();
//		
//        if(logger.isDebugEnabled()) {
//        	String msg = "";
//        	if(null != delResp) {
//	        	new StringBuffer("Deleted index <" + indexName +">, document type <" + typeName +
//	        			">, slice <" + sliceID + ">. Total records found <" + delResp.getTotalFound() + ">, deleted <" +
//	        			delResp.getTotalDeleted() +">, time taken <" + delResp.getTookInMillis() +" (ms)>.").toString();
//	        	logger.debug(msg);
//	        	System.out.println(msg);
//        	}
//        } 
//        
//        System.out.println(delResp.getTotalDeleted());
//	}
	
	public void deleteIndexBySlice(String indexName, String typeNames, String sliceID, boolean bWithoutDeletePlugin) {
		String msg = new StringBuffer("Preparing to delete alias <").append(sliceID).append("> for document type(s) <").append(typeNames).append(">.").toString();
		logger.info(msg);
		System.out.println(msg);

		IndicesAdminClient indicesAdminClient = esClient.admin().indices();
		
		if(null != indexName && null != typeNames) {
			String indexDocuments[] = typeNames.split(",");
			
			for (String typeName : indexDocuments) {
				try {
					long currentDateTime = System.currentTimeMillis();
					currentDateTime = currentDateTime/1000;			
					
					typeName = typeName.trim();
					String sliceIDStr = String.valueOf(sliceID);
					AliasesExistResponse aliasExistsResp = indicesAdminClient.prepareAliasesExist(sliceIDStr).get();
					if(aliasExistsResp.exists()) {	
						SearchRequestBuilder srb = esClient.prepareSearch(indexName)
								   .setTypes(typeName)
								   .setSize(ES_RESULT_SIZE)
								   .setSearchType(SearchType.QUERY_AND_FETCH)
//								   .addField("_id") //deprecated
								   .addStoredField("_id")
								   .setQuery(QueryBuilders.termsQuery("tenant", sliceIDStr));
						
						SearchResponse resp = srb.execute().actionGet();
						SearchHits hits = resp.getHits();
						if(null != hits && hits.getTotalHits() > 0) {
							deleteIndexBySliceAndID(indexName, typeName, sliceID, hits);
						} else {
							msg = new StringBuffer("No document was found to delete for slice <").append(sliceID).append(">, document type <").append(typeName).append(">.").toString();
							logger.info(msg);
							System.out.println(msg);
						}
						//NOT deleting the alias
						//deleteAlias(indexName, typeName, sliceID);
						dbUtil.deleteIndexStatusRecord(sliceID, typeName, currentDateTime);
					} else {
						msg = new StringBuffer("Alias for slice <").append(sliceID).append("> does not exist.").toString();
			        	logger.info(msg);
			        	System.out.println(msg);   			
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error(ex.getMessage());
				}				
			}
		}
	}	

	private void deleteIndexBySliceAndID(String parentIndexName,
			String documentType, String sliceID, SearchHits hits) {
		BulkRequestBuilder bulkRequest = esClient.prepareBulk();

		for (SearchHit searchHit : hits) {
			bulkRequest.add(esClient.prepareDelete(parentIndexName, documentType, searchHit.getId()));	
		}
				
		BulkResponse delResp = bulkRequest.get();
        if(!delResp.hasFailures()) {
        	String msg = new StringBuffer("Deleted documents from index <" ).append( parentIndexName ).append(">, document type <" ).append( documentType ).append(
        			">, slice <" ).append( sliceID ).append( ">. Time taken <" ).append( delResp.getTookInMillis() ).append(" (ms)>")
        			.append(", Records deleted <").append(delResp.getItems().length).append(">.").toString();

        	logger.debug(msg);
        	System.out.println(msg);
        } else {
        	String msg = new StringBuffer("Failed deleting documents from index <" ).append( parentIndexName ).append(">, document type <" ).append( documentType ).append(
        			">, slice <" ).append( sliceID ).append( ">. Time taken <" ).append( delResp.getTookInMillis() ).append(" (ms)>.").toString();
        	
        	logger.info(msg);
        	System.out.println(msg);   
        	
        	msg = delResp.buildFailureMessage();
        	logger.error(msg);
        	System.err.println(msg);
        }
	}
	
	private void deleteIndexBySliceAndID(String parentIndexName,
			String documentType, String sliceID) {
		BulkRequestBuilder bulkRequest = esClient.prepareBulk();

		for(long selectedID : getSelectedIDsToIndex()) {
			// either use client#prepare, or use Requests# to directly build index/delete requests
			String selectedIDFull = new StringBuilder().append(sliceID).append("_").append(documentType).append("_").append(selectedID).toString();
			bulkRequest.add(esClient.prepareDelete(parentIndexName, String.valueOf(sliceID), selectedIDFull));	
		}
		
		BulkResponse delResp = bulkRequest.get();
        //if(logger.isDebugEnabled()) {
        	String msg = new StringBuffer("Deleted index <" ).append( indexName ).append(">, document type <" ).append( documentType ).append(
        			">, slice <" ).append( sliceID ).append( ">. Time taken <" ).append( delResp.getTookInMillis() ).append(" (ms)>.").toString();
        	logger.info(msg);
        	System.out.println(msg);
        //}
	}	
	
//	private DeleteByQueryRequestBuilder newDeleteByQuery() {
//		return new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE);
//	}
//
//	private ElasticsearchClient client() {
//		return esClient.admin().indices();
//	}
	
	protected boolean isValidDocumentToIndex(String indexDocumentTypeAsSet, String currentIndex) {
		boolean bValidIndex = false;
		if(null != indexDocumentTypeAsSet) {
			if(indexDocumentTypeAsSet.indexOf(",") > 0) {
				String[] indexes = indexDocumentTypeAsSet.split(",");
				for (String index : indexes) {
					if(index.equalsIgnoreCase(currentIndex)) {
						bValidIndex = true;
						break;
					}
				}
			} else if (indexDocumentTypeAsSet.equalsIgnoreCase(currentIndex)) {
				bValidIndex = true;
			}
		} else {
			bValidIndex = true;
		}
		
		return bValidIndex;
	}
	
	
	/**
	 * This will delete for the given slice, for one or more document types
	 * @param sliceID
	 * @param docTypeOrTypes - comma separated document types in case more than one document types
	 * @return
	 */
	protected boolean deleteAllIndexDataForSlice(String sliceID, String docTypeOrTypes) {
		IndexManagerFactory imf = IndexManagerFactory.getInstance();
		imf.setIndexOperationType(IndexOperationType.Full);
		IndexManager im = imf.getIndexManager();
		IndexConfig configData = indexConfigReader.getIndexConfiguration();
		im.deleteIndexBySlice(configData.getParentIndexName(), docTypeOrTypes, sliceID, true);		
		
		return true;
	}	
	
//	public void deleteQuery(String indexName, String typeName, int sliceID) {
//		try {
//			IndicesAdminClient indicesAdminClient = tcp.admin().indices();
//			DeleteByQueryRequestBuilder builder = DeleteByQueryAction.INSTANCE.newRequestBuilder(indicesAdminClient);
//			builder.setIndices(indexName);
//			builder.setTypes(typeName);
//			builder.setQuery(QueryBuilders.
//			
//			QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
//			DeleteByQueryAction delAction = indicesAdminClient.prepareDelete(indexName).;
//			DeleteByQueryRequestBuilder deleteQuery = new DeleteByQueryRequestBuilder(indicesAdminClient, delAction).setIndices(indexName).setTypes(typeName)
//					.setQuery();
//			deleteQuery.setIndices(indexName);
//			if (typeName != null) {
//				deleteQuery.setTypes(typeName);
//			}
//			deleteQuery.setQuery(QueryBuilders.matchAllQuery());
//
//			deleteQuery.get();
//			System.out.println(String.format("Deleted index %s and type %s", indexName, typeName));
//		}
//		catch(Exception e) {
//			System.out.println(String.format("Failed to delete index, Index %s does not exist, continue any way", indexName));
//		}
//	}
	
	protected void deleteSelectedRecordsFromIndex(String parentIndexName) {
		IndicesAdminClient indicesAdminClient = esClient.admin().indices();
		
		if(null != indexDocumentType && null != selectedIDsToIndex && null != parentIndexName) {
			try {
				AliasesExistResponse aliasExistsResp = indicesAdminClient.prepareAliasesExist(String.valueOf(tenantID)).get();
				if(aliasExistsResp.exists()) {	
					BulkRequestBuilder bulkRequest = esClient.prepareBulk();

					for (Long selectedID : selectedIDsToIndex) {
						String selectedIDFull = new StringBuilder().append(tenantID).append("_").append(indexDocumentType).append("_").append(selectedID).toString();
						DeleteRequestBuilder drb = esClient.prepareDelete(parentIndexName, indexDocumentType, String.valueOf(selectedIDFull));
						if(indexDocumentType.equalsIgnoreCase("conversation")) {
							//int projectID = (int) jsonDataObj.get("resource_id");
							int projectID = 5011001; //TODO:: get the project id
							String projectKey = new StringBuffer(tenantID).append("_").append("project").append("_").append(projectID).toString();
							//jsonDataObj.put("parent", projectKey);
							drb.setParent(projectKey);
						}
						bulkRequest.add(drb);	
					}
							
					BulkResponse delResp = bulkRequest.get();
			        if(!delResp.hasFailures()) {
			        	String msg = new StringBuffer("Deleted documents from index <" ).append( parentIndexName ).append(">, document type <" ).append( indexDocumentType ).append(
			        			">, slice <" ).append( tenantID ).append( ">. Time taken <" ).append( delResp.getTookInMillis() ).append(" (ms)>")
			        			.append(", Records deleted <").append(delResp.getItems().length).append(">.").toString();

			        	logger.debug(msg);
			        	System.out.println(msg);
			        } else {
			        	String msg = new StringBuffer("Failed deleting documents from index <" ).append( parentIndexName ).append(">, document type <" ).append( indexDocumentType ).append(
			        			">, slice <" ).append( tenantID ).append( ">. Time taken <" ).append( delResp.getTookInMillis() ).append(" (ms)>.").toString();
			        	
			        	logger.info(msg);
			        	System.out.println(msg);   
			        	
			        	msg = delResp.buildFailureMessage();
			        	logger.error(msg);
			        	System.err.println(msg);
			        }					
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				logger.error(ex.getMessage());
			}
		}
	}	
}
