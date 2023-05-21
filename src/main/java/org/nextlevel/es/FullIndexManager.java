package org.nextlevel.es;

import java.sql.Connection;
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
public final class FullIndexManager extends IndexManager {
	private static Logger logger = LoggerFactory.getLogger(FullIndexManager.class);
	
	private static HashMap<String, TenantDBConnectionData> sliceDBMap;
	
	public FullIndexManager() {
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
					System.out.println("Doing threaded indexing...");
					long currentTime = System.currentTimeMillis();
					
					doFullIndexForAllSlicesThreaded();
					
					long endTime = System.currentTimeMillis();
					long timeTaken = (endTime-currentTime)/(1000*60);
					System.out.println("TIME TAKEN (MIN): " + timeTaken);
					logger.info("TIME TAKEN (MIN): " + timeTaken);
				} else {
					doFullIndexForAllSlices();
				}
			} else { //slice specific indexing
				if(isMultiThreadingEnabled) {
					System.out.println("Doing threaded indexing ...");
					
					doFullIndexForSingleSliceThreaded(tenantID);
				} else {
					doFullIndexForSingleSlice(tenantID);
				}
			}
		} catch (Exception e) {
			bReturn = false;
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
		return bReturn;
	}

	//TODO::Multi-threaded per slice
	private void doFullIndexForAllSlices() {
		Connection platformDBConnection = null;
		TenantDBConnectionData tenantDBConnData = null;
		try {
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			sliceDBMap = dbUtil.getSliceDBsDetails();
			Set<String> sliceKeys = sliceDBMap.keySet();
			for (String tenantID : sliceKeys) {
				//First delete all documents from the index - before doing full indexing
				deleteAllIndexDataForSlice(tenantID, indexDocumentType);
				
				//Now do full indexing
				Connection sliceDBConn = null;
				try {
					Tenant tenant = tenantsConfigData.getTenant(tenantID);
					tenantDBConnData = sliceDBMap.get(tenantID);
					sliceDBConn = dbUtil.getTenantDBConnection(tenantDBConnData);
					if(null != sliceDBConn) {
						boolean bIsTenantConfigESSearchEnabled = tenant.isElasticSearchEnabled(); 
						if(!bIsTenantConfigESSearchEnabled) {
							String msg = "ElasticSearch is not turned on for tenant: " + tenantID +". Skipping indexing...";
							System.out.println(msg);
							logger.warn(msg);
							continue;
						}
						
						System.out.println("Connected to slice: " + tenantID);
						logger.info("Full Indexing, connected to slice: " + tenantID);
						
						for (String indexName : indexKeys) {
							try {
								if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
									if(logger.isDebugEnabled()) {
										logger.debug(indexName + " not set for indexing..so skipped.");
									}
									continue;
								}

//								if(dbUtil.isAlreadyIndexing(platformDBConnection, tenantID, indexName)) {
//									String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
//											append(indexName).append(">, for slice: <").append(tenantID).append(">.").toString();
//									logger.info(msg);
//									System.out.println(msg);
//								} else {
//									dbUtil.updateIndexInitiatedStatus(tenantID, indexName, IndexOperationType.Full);
									
									Index index = indexMap.get(indexName);
									String json = dbUtil.runQueryToJSON(sliceDBConn, index.getQuery(), tenantID, index);
									
									if(logger.isDebugEnabled()) {
										if(null != json) {
											logger.debug("==================== Full Index :: Data ====================");
											logger.debug(json);
										}
									}
									
									JSONObject jsnobject = new JSONObject(json);
									JSONArray jsonArray = jsnobject.getJSONArray("results");
									bulkIndex(jsonArray, index);
//									dbUtil.updateIndexCompletionStatus(tenantID, indexName, IndexStatus.Completed);
//								}
							} catch (Exception ex) {
								ex.printStackTrace();
								logger.error(ex.getMessage());
//								dbUtil.updateIndexCompletionStatus(tenantID, indexName, IndexStatus.Failed);
							}
						}				
					}
				} finally {
					createAlias(tenantID, indexConfigReader.getIndexConfiguration().getParentIndexName());
					dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			//dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
		}
	}

	//TODO::Refactoring of common code between doFullIndexForAllSlices, doFullIndexForSingleSlice
	private void doFullIndexForSingleSlice(String tenantID) {
		//First delete all documents from the index - before doing full indexing
		deleteAllIndexDataForSlice(tenantID, indexDocumentType);
		TenantDBConnectionData tenantDBConnData = null;

		//Now do full indexing
		Connection sliceDBConn = null;		
		Connection platformDBConnection = null;
		try {
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			DatabaseUtil dbUtil = new DatabaseUtil();
			sliceDBMap = dbUtil.getSliceDBsDetails();
			tenantDBConnData = sliceDBMap.get(tenantID);
			sliceDBConn = dbUtil.getTenantDBConnection(sliceDBMap.get(tenantID));
			//platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
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
				logger.info("Full Indexing, connected to slice: " + tenantID);
				
				for (String indexName : indexKeys) {
					try {
						if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
							if(logger.isDebugEnabled()) {
								logger.debug(indexName + " not set for indexing..so skipped.");
							}
							continue;
						}
						
//						if(dbUtil.isAlreadyIndexing(platformDBConnection, sliceID, indexName)) {
//							String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
//									append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
//							logger.info(msg);
//							System.out.println(msg);
//						} else {
//							dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Full);
							
							Index index = indexMap.get(indexName);
							String json = dbUtil.runQueryToJSON(sliceDBConn, index.getQuery(), tenantID, index);
							if(logger.isDebugEnabled()) {
								if(null != json) {
									logger.debug("==================== Full Index :: Data ====================");
									logger.debug(json);
								}
							}
							
							JSONObject jsnobject = new JSONObject(json);
							JSONArray jsonArray = jsnobject.getJSONArray("results");
							bulkIndex(jsonArray, index);
//							dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Completed);
//						}
					} catch (Exception ex) {
						ex.printStackTrace();
						logger.error(ex.getMessage());
//						dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
					}
				}			
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			createAlias(tenantID, indexConfigReader.getIndexConfiguration().getParentIndexName());
			dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());	
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
		}
	}	
	
	
	private void doFullIndexForAllSlicesThreaded() {
		Connection platformDBConnection = null;
		TenantDBConnectionData tenantDBConnData = null;

		try {
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			sliceDBMap = dbUtil.getSliceDBsDetails();
			Set<String> sliceKeys = sliceDBMap.keySet();
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
			for (String sliceID : sliceKeys) {
				//First delete all documents from the index - before doing full indexing
				deleteAllIndexDataForSlice(sliceID, indexDocumentType);
				
				//Now do full indexing
				Connection sliceDBConn = null;
				try {
					tenantDBConnData = sliceDBMap.get(sliceID);
					sliceDBConn = dbUtil.getTenantDBConnection(tenantDBConnData);
					if(null != sliceDBConn) {
//						Tenant tenant = tenantsConfigData.getTenant(tenantID);
						Tenant tenant = tenantsConfigData.getTenant(sliceID);
						boolean bIsTenantConfigESSearchEnabled = tenant.isElasticSearchEnabled(); 
						if(!bIsTenantConfigESSearchEnabled) {
							String msg = "ElasticSearch is not turned on for tenant: " + tenantID +". Skipping indexing...";
							System.out.println(msg);
							logger.warn(msg);
							continue;
						}
						
						System.out.println("Connected to slice: " + sliceID);
						logger.info("Full Indexing, connected to slice: " + sliceID);
						
						for (String indexName : indexKeys) {
							try {
								if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
									if(logger.isDebugEnabled()) {
										logger.debug(indexName + " not set for indexing..so skipped.");
									}
									continue;
								}
								
								if(indexUtil.isAlreadyIndexing(sliceID, indexName)) {
									String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
											append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
									logger.info(msg);
									System.out.println(msg);
								} else {
									indexUtil.updateIndexStatus(sliceID, indexName, IndexOperationType.Full, IndexStatus.Running);
									
									Index index = indexMap.get(indexName);
									ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
									tim.indexData(index, sliceID, IndexOperationType.Full, 0);								}
							} catch (Exception ex) {
								ex.printStackTrace();
								logger.error(ex.getMessage());
								indexUtil.updateIndexStatus(sliceID, indexName, IndexOperationType.Full, IndexStatus.Failed);
							}
						}				
					}
				} finally {
					createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
					dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
		}
	}

	//TODO::Refactoring of common code between doFullIndexForAllSlices, doFullIndexForSingleSlice
	private void doFullIndexForSingleSliceThreaded (String sliceID) {
		TenantDBConnectionData tenantDBConnData = null;

		//First delete all documents from the index - before doing full indexing
		deleteAllIndexDataForSlice(sliceID, indexDocumentType);
		
		//Now do full index
		Connection sliceDBConn = null;		
		try {
			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
			Set<String> indexKeys = indexMap.keySet(); 
			
			sliceDBMap = dbUtil.getSliceDBsDetails();
			tenantDBConnData = sliceDBMap.get(sliceID);
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
				
				System.out.println("Connected to slice: " + sliceID);
				logger.info("Full Indexing, connected to slice: " + sliceID);
				
				for (String indexName : indexKeys) {
					try {
						if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
							if(logger.isDebugEnabled()) {
								logger.debug(indexName + " not set for indexing..so skipped.");
							}
							continue;
						}
						
						if(indexUtil.isAlreadyIndexing(sliceID, indexName)) {
							String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
									append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
							logger.info(msg);
							System.out.println(msg);
						} else {
							indexUtil.updateIndexStatus(sliceID, indexName, IndexOperationType.Full, IndexStatus.Running);
							
							Index index = indexMap.get(indexName);
							ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
							tim.indexData(index, sliceID, IndexOperationType.Full, 0);							
						}
					} catch (Exception ex) {
						ex.printStackTrace();
						logger.error(ex.getMessage());
						indexUtil.updateIndexStatus(sliceID, indexName, IndexOperationType.Full, IndexStatus.Failed);
					}
				}			
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
			dbUtil.closeDBConnection(sliceDBConn, tenantDBConnData.getDbType());	
		}
	}

	@Override
	public boolean indexNearRealTime() {
		// TODO Auto-generated method stub
		return false;
	}		
}
