package org.nextlevel.es;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.config.Index;
import org.nextlevel.es.config.IndexConfig;
import org.nextlevel.es.config.IndexConfigReader;
import org.nextlevel.es.config.IndexUtil;
import org.nextlevel.es.db.util.DatabaseUtil;
import org.nextlevel.es.db.util.TenantDBConnectionData;
import org.nextlevel.es.tc.TransportClientProvider;

/**
 * 
 * @author nextlevel
 *
 */
public final class ThreadedIndexManager {
	private static Logger logger = LoggerFactory.getLogger(ThreadedIndexManager.class);
	
	private static ThreadedIndexManager instance = new ThreadedIndexManager();
	private final long EXECUTOR_SHUTDOWN_TIME = TimeUnit.SECONDS.toMillis(10);
	
    private ExecutorService execService;
	private DatabaseUtil dbUtil;
	private IndexConfigReader indexConfigReader;
	private IndexConfig indexConfig;
	private HashMap<String, TenantDBConnectionData> sliceDBMap;
	private IndexUtil indexUtil = new IndexUtil();

	private ThreadedIndexManager() {
		dbUtil = new DatabaseUtil();
		indexConfigReader = IndexConfigReader.getInstance();
		indexConfig = indexConfigReader.getIndexConfiguration();
		sliceDBMap = dbUtil.getSliceDBsDetails();
		System.out.println("TOTAL COREs: " + Runtime.getRuntime().availableProcessors());
		//execService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		//addShutdownHook();
	}
	
	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				execService.shutdown();
				try {
					if(!execService.awaitTermination(EXECUTOR_SHUTDOWN_TIME, TimeUnit.SECONDS)) {
						logger.warn("Threaded indexer didn't terminate in the specified time. Some of the indexing threads may still be running.");
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
		});		
	}

	public static ThreadedIndexManager getInstance() {
		return instance;
	}
	
	public void indexData(Index index, String tenantID, IndexOperationType opType, long lLastIndexRunTime) {
		TenantDBConnectionData tenantDBConnData = null;

		try {
			execService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());	
			List<Future<ThreadedIndexData>> listTasks = new ArrayList<Future<ThreadedIndexData>>();
		    
			int batchCount = indexConfig.getBatchCount();
			int count = 1, total = batchCount;
			int currentStart = 0, currentEnd = batchCount;
			while(total == batchCount) {	
				Connection sliceDBConn = null;
				try {
					tenantDBConnData = fetchTenantDBConnectionData(tenantID);
					sliceDBConn = fetchSliceDBConnection(tenantID, tenantDBConnData);
					String json = null;
					
					switch(opType) {
					case Full:
						json = dbUtil.runQueryToJSONInBatches(sliceDBConn, index.getQuery(), tenantID, index.getModifiedDateKey(), currentStart, currentEnd, index);
						break;
						
					case Incremental:
						json = dbUtil.runQueryToJSONInBatches(sliceDBConn, index.getQuery(), tenantID, index.getModifiedDateKey(), currentStart, currentEnd, opType, lLastIndexRunTime, index);
						break;
						
						default:
							throw new Exception(opType.getValue() +" not supported.");
					}
					
					if(logger.isDebugEnabled()) {
						if(null != json) {
							logger.debug("==================== Full Index :: Data ====================");
							logger.debug(json);
							//System.out.println(json);
						}
					}
		
					JSONObject jsnobject = new JSONObject(json);
					JSONArray jsonArray = jsnobject.getJSONArray("results");
	
					total = jsonArray.length();
					
					if(total > 0) {
						String msg = new StringBuffer("======= Slice: ").append(tenantID).append("(").append(count).append("), Index: ").append(index.getDocumentType())
								.append(", Current batch records: ").append(total).append(", Per batch: ").append(indexConfigReader.getBatchCount())
								.append(".").toString();
						//System.out.println(msg);
						logger.info(msg);
						
						ThreadedIndexPool task = new ThreadedIndexPool(jsonArray, index, tenantID);
						Future<ThreadedIndexData> futureTask = execService.submit(task);			
						listTasks.add(futureTask);
					}
				} catch (Exception ex) {
					logger.error(ex.getMessage());
					ex.printStackTrace();
				} finally {
					dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());
				}
				
				currentStart = currentEnd;
				currentEnd += batchCount;
				count += 1;
			}
			
			for (Future<ThreadedIndexData> task : listTasks) {
				try {
					ThreadedIndexData returnData = task.get();
					logger.info("Batch Return Code: "+ returnData.isbReturnData());
				} catch (InterruptedException | ExecutionException e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
			
			indexUtil.updateIndexStatus(tenantID, index.getDocumentType(), opType, IndexStatus.Completed);	
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			if(null != execService) {
				execService.shutdown();
			}
		}
	}

	private TenantDBConnectionData fetchTenantDBConnectionData(String tenantID) throws Exception {
		TenantDBConnectionData tenantDBConnData = sliceDBMap.get(tenantID);
		if(null == tenantDBConnData) {
			sliceDBMap = dbUtil.getSliceDBsDetails();
			tenantDBConnData = sliceDBMap.get(tenantID);
		}
		
		if(null == tenantDBConnData) {
			throw new Exception("Invalid sliceID found <" + tenantID + ">.");
		}
		
		return tenantDBConnData;
	}
	
	private Connection fetchSliceDBConnection(String sliceID, TenantDBConnectionData tenantDBConnData) throws Exception {
		//return dbUtil.getSliceSecondaryDBConnection(sliceDBConnData);
		return dbUtil.getTenantDBConnection(tenantDBConnData);
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
}


class ThreadedIndexPool implements Callable<ThreadedIndexData> {
	private static Logger logger = LoggerFactory.getLogger(ThreadedIndexPool.class);
	
	private IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
	private TransportClientProvider tcp = TransportClientProvider.getInstance();
	protected Client esClient = null;

	private JSONArray jsonArray;
	private Index index;
	private ThreadedIndexData resultData;

	private String tenantID;

	public ThreadedIndexPool(JSONArray jsonArray, Index index, String tenantID) {
		this.tenantID = tenantID;
		try {
			esClient = tcp.getTransportClient();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			System.err.println(e.getMessage());
		}
		this.jsonArray = jsonArray;
		this.index = index;
		this.resultData = new ThreadedIndexData();
	}

	@Override
	public ThreadedIndexData call() throws Exception {
		bulkIndex(jsonArray, index);		
		return resultData;
	}
	
//	protected void bulkIndex(JSONArray jsonArray, Index index) {
//		logger.info("Initiated bulk index for: " + index.getName());
//		
//		BulkRequestBuilder bulkRequest = esClient.prepareBulk();
//		String parentIndexName = indexConfigReader.getParentIndexName(index);
//		
//		boolean bReturn = true;
//		
//		for(int i=0; i<jsonArray.length(); i++) {
//			JSONObject jsonDataObj = (JSONObject) jsonArray.get(i);
//			jsonDataObj.put("tenant", tenantID);
//			
//			String idValue = getIDValue(jsonDataObj, index.getIDKey(), index);
//			if(null != idValue) {
//				bulkRequest.add(esClient.prepareIndex(parentIndexName, index.getName(), idValue)
//				        .setSource(jsonDataObj.toString())
//				        );
//				
//				if(logger.isDebugEnabled()) {
//					logger.debug("Added for Bulk Index: " + index.getName() + ", Data: "+ jsonDataObj.toString());
//				}
//				
//				System.out.println("Added for Bulk Index: " + idValue + "::" + index.getName() + ", Data: "+ jsonDataObj.toString());
//			}
//		}
//		
//		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
//		if (bulkResponse.hasFailures()) {
//			bReturn = false;
//			logger.error(bulkResponse.buildFailureMessage());
//		}	
//		
//		TimeValue timeTaken = bulkResponse.getTook();
//		String msg = new StringBuffer("Time taken (seconds): ").append((float)timeTaken.getMillis()/1000).append(", Per Document (ms): ").append((timeTaken.getMillis()/jsonArray.length()))  
//				.toString();
//		System.out.println(msg);
//		logger.info(msg);
//		
//		resultData.setbReturnData(bReturn);
//		resultData.setBulkResponse(bulkResponse);
//	}	
	
	protected void bulkIndex(JSONArray jsonArray, Index index) {
		logger.info("Initiated bulk index for: " + index.getName());
		
		BulkRequestBuilder bulkRequest = esClient.prepareBulk();
		String parentIndexName = indexConfigReader.getParentIndexName(index);
		
		boolean bReturn = true;
		
		for(int i=0; i<jsonArray.length(); i++) {
			JSONObject jsonDataObj = (JSONObject) jsonArray.get(i);
			jsonDataObj.put("tenant", tenantID);
			
			String idValue = getIDValue(jsonDataObj, index.getIDKey(), index);
			if(null != idValue) {
				String projectKey = null;
				if(index.getName().equalsIgnoreCase("conversation")) {
					String tenant = jsonDataObj.getString("tenant");
					int projectID = (int) jsonDataObj.get("resource_id");
					projectKey = new StringBuffer(tenant).append("_").append("project").append("_").append(projectID).toString();
					//jsonDataObj.put("parent", projectKey);
				}
				
				if(null != projectKey) {
					bulkRequest.add(esClient.prepareIndex(parentIndexName, index.getName(), idValue)
					        .setSource(jsonDataObj.toString())
					        .setParent(projectKey)
					        );
				} else {
					bulkRequest.add(esClient.prepareIndex(parentIndexName, index.getName(), idValue)
					        .setSource(jsonDataObj.toString())
					        );
				}
				
				if(logger.isDebugEnabled()) {
					logger.debug("Added for Bulk Index: " + index.getName() + ", Data: "+ jsonDataObj.toString());
				}
				
				//System.out.println("Added for Bulk Index: " + idValue + "::" + index.getName() + ", Data: "+ jsonDataObj.toString());
			}
		}
		
		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
			bReturn = false;
			logger.error(bulkResponse.buildFailureMessage());
			System.err.println("BULK REQUEST HAS ERRORS----------");
			System.err.println(bulkResponse.buildFailureMessage());
		}	
		
		TimeValue timeTaken = bulkResponse.getTook();
		String msg = new StringBuffer("Time taken (seconds): ").append((float)timeTaken.getMillis()/1000).append(", Per Document (ms): ").append((timeTaken.getMillis()/jsonArray.length()))  
				.toString();
		System.out.println(msg);
		logger.info(msg);
		
		resultData.setbReturnData(bReturn);
		resultData.setBulkResponse(bulkResponse);
	}
	
	protected String getIDValue(JSONObject jsonDataObj, String idKey, Index index) {
		StringBuilder idValue = new StringBuilder();
		idValue.append(tenantID).append("_").append(index.getDocumentType()).append("_");

		try {
			//idValue = String.valueOf(jsonDataObj.getInt(idKey));
			idValue.append(jsonDataObj.getInt(idKey));
		} catch (Exception ex) {
			if(idKey.indexOf(".")>=0) {
				idKey = idKey.substring(idKey.indexOf(".")+1);
			}
			
			try {
				//idValue = String.valueOf(jsonDataObj.getInt(idKey));
				idValue.append(jsonDataObj.getInt(idKey));
			} catch (Exception ex2) {
				logger.error("Cannot get the value for row_id column...");
			}
		}
		
		return idValue.toString();
	}	
}

class ThreadedIndexData {
	private boolean bReturnData;
	private BulkResponse bulkResponse;

	public ThreadedIndexData() {
	}

	public boolean isbReturnData() {
		return bReturnData;
	}

	public void setbReturnData(boolean bReturnData) {
		this.bReturnData = bReturnData;
	}

	public BulkResponse getBulkResponse() {
		return bulkResponse;
	}

	public void setBulkResponse(BulkResponse bulkResponse) {
		this.bulkResponse = bulkResponse;
	}
}
