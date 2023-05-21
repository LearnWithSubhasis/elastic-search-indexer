package org.nextlevel.es;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.config.Index;
import org.nextlevel.es.config.IndexConfigReader;
import org.nextlevel.es.db.util.DatabaseUtil;
import org.nextlevel.es.db.util.TenantDBConnectionData;
import org.nextlevel.es.tenants.Tenant;

/**
 * 
 * @author nextlevel
 *
 */
public final class IncrementalIndexManager extends IndexManager {
	private static Logger logger = LoggerFactory.getLogger(IncrementalIndexManager.class);
	
	public IncrementalIndexManager() {
		super();
		try {
			esClient = tcp.getTransportClient();
			initializeIndexAndMappings();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			System.err.println(e.getMessage());
		}
	}

	@Override
	public boolean index() {
		boolean bReturn = true;
		boolean isMultiThreadingEnabled = indexConfigReader.getIndexConfiguration().isMultiThreaded();
		
		try {
			//if slice is not set, it is for all the slices
			if(null == tenantID) {
				if(isMultiThreadingEnabled) {
					System.out.println("Doing threaded indexing (+) ...");
					long currentTime = System.currentTimeMillis();
					
					doIncrementalIndexForAllSlicesThreaded();
					
					long endTime = System.currentTimeMillis();
					long timeTaken = (endTime-currentTime)/(1000*60);
					System.out.println("TIME TAKEN (MIN): " + timeTaken);
					logger.info("TIME TAKEN (MIN): " + timeTaken);
				} else {
					doIncrementalIndexForAllSlices();
				}
				
			} else { //slice specific indexing
				if(isMultiThreadingEnabled) {
					System.out.println("Doing threaded indexing (+) ...");
					
					doIncrementalIndexForSingleSliceThreaded(tenantID);
				} else {
					doIncrementalIndexForSingleSlice(tenantID);
				}
			}
		} catch (Exception e) {
			bReturn = false;
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
		return bReturn;
	}

	private void doIncrementalIndexForAllSlices() {
		TenantDBConnectionData tenantDBConnData = null;

		try {
			SliceIndexMetadata slicesIndexMetadata = indexUtil.getSlicesIndexMetaData();
			
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			DatabaseUtil dbUtil = new DatabaseUtil();
			HashMap<String, TenantDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
			Set<String> sliceKeys = sliceDBMap.keySet();
			for (String tenantID : sliceKeys) {
				Connection sliceDBConn = null;
				try {
					sliceDBMap = dbUtil.getSliceDBsDetails();
					tenantDBConnData = sliceDBMap.get(tenantID);
					sliceDBConn = dbUtil.getTenantDBConnection(tenantDBConnData);

					if(null != sliceDBConn) {
						Tenant tenant = tenantsConfigData.getTenant(tenantID);
						boolean bIsTenantConfigESSearchEnabled = tenant.isElasticSearchEnabled(); 
						if(!bIsTenantConfigESSearchEnabled) {
							String msg = "ElasticSearch is not turned on for tenant: " + tenantID +". Skipping indexing...";
							System.out.println(msg);
							logger.warn(msg);
							continue;
						}
						
						System.out.println("Connected to slice: " + tenantID);
						logger.info("Incremental Indexing, connected to slice: " + tenantID);
						
						for (String indexName : indexKeys) {
							try {
								if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
									if(logger.isDebugEnabled()) {
										logger.debug(indexName + " not set for indexing..so skipped.");
									}
									continue;
								}
								
								if(indexUtil.isAlreadyIndexing(tenantID, indexName)) {
									String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
											append(indexName).append(">, for slice: <").append(tenantID).append(">.").toString();
									logger.info(msg);
									System.out.println(msg);
								} else {
									indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Running);
									
									long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(tenantID, indexName);
									
									String msg = new StringBuffer("Initiating task for index: ").append(indexName).append(", last time indexed on: ").append(lLastIndexRunTime).toString(); 
									logger.info(msg);
									System.out.println(msg);
									
									Index index = indexMap.get(indexName);
									String json = dbUtil.runQueryToJSON(sliceDBConn, index, lLastIndexRunTime, tenantID);
									if(null != json) {
										if(logger.isDebugEnabled()) {
											logger.debug("==================== Incremental Index :: Data ====================");
											logger.debug(json);
										}
									}
									
									JSONObject jsnobject = new JSONObject(json);
									JSONArray jsonArray = jsnobject.getJSONArray("results");
									bulkIndex(jsonArray, index);
									
									indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Completed);
								}
							} catch (Exception ex) {
								ex.printStackTrace();
								logger.error(ex.getMessage());
								indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Failed);
							}
						}				
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error(ex.getMessage());
				} finally {
					createAlias(tenantID, indexConfigReader.getIndexConfiguration().getParentIndexName());
					dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());	
				}
			}			
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}		
	}

	private void doIncrementalIndexForSingleSlice(String tenantID) throws SQLException {
		TenantDBConnectionData tenantDBConnData = null;
		Connection sliceDBConn = null;
		try {
			SliceIndexMetadata slicesIndexMetadata = indexUtil.getSlicesIndexMetaData();
			
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			HashMap<String, TenantDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
			sliceDBMap = dbUtil.getSliceDBsDetails();
			tenantDBConnData = sliceDBMap.get(tenantID);
			sliceDBConn = dbUtil.getTenantDBConnection(tenantDBConnData);

			if(null != sliceDBConn) {
				Tenant tenant = tenantsConfigData.getTenant(tenantID);
				boolean bIsTenantConfigESSearchEnabled = tenant.isElasticSearchEnabled(); 
				if(!bIsTenantConfigESSearchEnabled) {
					String msg = "ElasticSearch is not turned on for tenant: " + tenantID +". Skipping indexing...";
					System.out.println(msg);
					logger.warn(msg);
					return;
				}
				
				System.out.println("Connected to slice: " + tenantID);
				logger.info("Incremental Indexing, connected to slice: " + tenantID);
				
				for (String indexName : indexKeys) {
					try {
						if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
							if(logger.isDebugEnabled()) {
								logger.debug(indexName + " not set for indexing..so skipped.");
							}
							continue;
						}
						
						if(indexUtil.isAlreadyIndexing(tenantID, indexName)) {
							String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
									append(indexName).append(">, for slice: <").append(tenantID).append(">.").toString();
							logger.info(msg);
							System.out.println(msg);
						} else {
							indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Running);
							
							long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(tenantID, indexName);
							
							Index index = indexMap.get(indexName);
							String json = dbUtil.runQueryToJSON(sliceDBConn, index, lLastIndexRunTime, tenantID);
							if(null != json) {
								if(logger.isDebugEnabled()) {
									logger.debug("==================== Incremental Index :: Data ====================");
									logger.debug(json);
								}
							}
							
							JSONObject jsnobject = new JSONObject(json);
							JSONArray jsonArray = jsnobject.getJSONArray("results");
							bulkIndex(jsonArray, index);
							
							indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Completed);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
						logger.error(ex.getMessage());
						indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Failed);
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			createAlias(tenantID, indexConfigReader.getIndexConfiguration().getParentIndexName());
			dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());	
		}			
	}
	
	private void doIncrementalIndexForAllSlicesThreaded() {
		TenantDBConnectionData tenantDBConnData = null;
		
		try {
			SliceIndexMetadata slicesIndexMetadata = indexUtil.getSlicesIndexMetaData();
			
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			DatabaseUtil dbUtil = new DatabaseUtil();
			HashMap<String, TenantDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
			Set<String> sliceKeys = sliceDBMap.keySet();
			for (String tenantID : sliceKeys) {
				Connection sliceDBConn = null;
				try {
					tenantDBConnData = sliceDBMap.get(tenantID);
					sliceDBConn = dbUtil.getTenantDBConnection(tenantDBConnData);

					if(null != sliceDBConn) {
						Tenant tenant = tenantsConfigData.getTenant(tenantID);
						boolean bIsTenantConfigESSearchEnabled = tenant.isElasticSearchEnabled(); 
						if(!bIsTenantConfigESSearchEnabled) {
							String msg = "ElasticSearch is not turned on for tenant: " + tenantID +". Skipping indexing...";
							System.out.println(msg);
							logger.warn(msg);
							continue;
						}
						
						System.out.println("Connected to slice: " + tenantID);
						logger.info("Incremental Indexing, connected to slice: " + tenantID);
						
						for (String indexName : indexKeys) {
							try {
								if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
									if(logger.isDebugEnabled()) {
										logger.debug(indexName + " not set for indexing..so skipped.");
									}
									continue;
								}
								
								if(indexUtil.isAlreadyIndexing(tenantID, indexName)) {
									String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
											append(indexName).append(">, for slice: <").append(tenantID).append(">.").toString();
									logger.info(msg);
									System.out.println(msg);
								} else {
									indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Running);
									
									long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(tenantID, indexName);
									
									String msg = new StringBuffer("Initiating task for index: ").append(indexName).append(", last time indexed on: ").append(lLastIndexRunTime).toString(); 
									logger.info(msg);
									System.out.println(msg);
									
									Index index = indexMap.get(indexName);
									ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
									tim.indexData(index, tenantID, IndexOperationType.Incremental, lLastIndexRunTime);						
								}
							} catch (Exception ex) {
								ex.printStackTrace();
								logger.error(ex.getMessage());
								indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Failed);
							}
						}				
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error(ex.getMessage());
				} finally {
					createAlias(tenantID, indexConfigReader.getIndexConfiguration().getParentIndexName());
					dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());	
				}
			}			
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}	
	}
	
	private void doIncrementalIndexForSingleSliceThreaded(String tenantID) {
		Connection sliceDBConn = null;
		TenantDBConnectionData tenantDBConnData = null;
		
		try {
			SliceIndexMetadata slicesIndexMetadata = indexUtil.getSlicesIndexMetaData();
			
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			HashMap<String, TenantDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
			tenantDBConnData = sliceDBMap.get(tenantID);
			sliceDBConn = dbUtil.getTenantDBConnection(tenantDBConnData);

			if(null != sliceDBConn) {
				Tenant tenant = tenantsConfigData.getTenant(tenantID);
				boolean bIsTenantConfigESSearchEnabled = tenant.isElasticSearchEnabled(); 
				if(!bIsTenantConfigESSearchEnabled) {
					String msg = "ElasticSearch is not turned on for tenant: " + tenantID +". Skipping indexing...";
					System.out.println(msg);
					logger.warn(msg);
					return;
				}
				
				System.out.println("Connected to slice: " + tenantID);
				logger.info("Incremental Indexing, connected to slice: " + tenantID);
				
				for (String indexName : indexKeys) {
					try {
						if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
							if(logger.isDebugEnabled()) {
								logger.debug(indexName + " not set for indexing..so skipped.");
							}
							continue;
						}
						
						if(indexUtil.isAlreadyIndexing(tenantID, indexName)) {
							String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
									append(indexName).append(">, for slice: <").append(tenantID).append(">.").toString();
							logger.info(msg);
							System.out.println(msg);
						} else {
							indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Running);
							
							long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(tenantID, indexName);
							
							Index index = indexMap.get(indexName);
							ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
							tim.indexData(index, tenantID, IndexOperationType.Incremental, lLastIndexRunTime);						
						}
					} catch (Exception ex) {
						ex.printStackTrace();
						logger.error(ex.getMessage());
						indexUtil.updateIndexStatus(tenantID, indexName, IndexOperationType.Incremental, IndexStatus.Failed);
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			createAlias(tenantID, indexConfigReader.getIndexConfiguration().getParentIndexName());
			dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());	
		}				
	}

	@Override
	public boolean indexNearRealTime() {
		// TODO Auto-generated method stub
		return false;
	}
}
