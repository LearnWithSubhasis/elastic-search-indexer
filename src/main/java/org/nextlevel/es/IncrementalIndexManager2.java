package org.nextlevel.es;
//package org.nextlevel.es;
//
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.Set;
//
//import org.json.JSONArray;
//import org.json.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.nextlevel.es.config.Index;
//import org.nextlevel.es.config.IndexConfigReader;
//import org.nextlevel.es.db.util2.DBType;
//import org.nextlevel.es.db.util2.DatabaseUtil;
//import org.nextlevel.es.db.util2.SliceDBConnectionData;
//
///**
// * 
// * @author nextlevel
// *
// */
//public final class IncrementalIndexManager2 extends IndexManager {
//	private static Logger logger = LoggerFactory.getLogger(IncrementalIndexManager2.class);
//	
//	public IncrementalIndexManager2() {
//		super();
//		try {
//			esClient = tcp.getTransportClient();
//			initializeIndexAndMappings();
//		} catch (Exception e) {
//			e.printStackTrace();
//			logger.error(e.getMessage());
//			System.err.println(e.getMessage());
//		}
//	}
//
//	@Override
//	public boolean index() {
//		boolean bReturn = true;
//		boolean isMultiThreadingEnabled = indexConfigReader.getIndexConfiguration().isMultiThreaded();
//		
//		try {
//			//if slice is not set, it is for all the slices
//			if(sliceID < 1) {
//				if(isMultiThreadingEnabled) {
//					System.out.println("Doing threaded indexing (+) ...");
//					long currentTime = System.currentTimeMillis();
//					
//					doIncrementalIndexForAllSlicesThreaded();
//					
//					long endTime = System.currentTimeMillis();
//					long timeTaken = (endTime-currentTime)/(1000*60);
//					System.out.println("TIME TAKEN (MIN): " + timeTaken);
//					logger.info("TIME TAKEN (MIN): " + timeTaken);
//				} else {
//					doIncrementalIndexForAllSlices();
//				}
//				
//			} else { //slice specific indexing
//				if(isMultiThreadingEnabled) {
//					System.out.println("Doing threaded indexing (+) ...");
//					
//					doIncrementalIndexForSingleSliceThreaded(sliceID);
//				} else {
//					doIncrementalIndexForSingleSlice(sliceID);
//				}
//			}
//		} catch (Exception e) {
//			bReturn = false;
//			logger.error(e.getMessage());
//			e.printStackTrace();
//		}
//		
//		return bReturn;
//	}
//
//	private void doIncrementalIndexForAllSlices() {
//		Connection platformDBConnection = null;
//	
//		try {
//			SliceIndexMetadata slicesIndexMetadata = dbUtil.getSlicesIndexMetaData();
//			
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			HashMap<Integer, SliceDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
//			Set<Integer> sliceKeys = sliceDBMap.keySet();
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
//			for (Integer sliceID : sliceKeys) {
//				Connection sliceDBConn = null;
//				try {
//					sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//					if(null != sliceDBConn) {
//						boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigESSearchEnabledForSlice(sliceDBConn, sliceID, false);
//						if(!bIsSliceConfigESSearchEnabled) {
//							String msg = "Slice config <ENABLE_ELASTIC_SEARCH> is not turned on for slice: " + sliceID +". Skipping indexing...";
//							System.out.println(msg);
//							logger.warn(msg);
//							continue;
//						}
//						
//						System.out.println("Connected to slice: " + sliceID);
//						logger.info("Incremental Indexing, connected to slice: " + sliceID);
//						
//						for (String indexName : indexKeys) {
//							try {
//								if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
//									if(logger.isDebugEnabled()) {
//										logger.debug(indexName + " not set for indexing..so skipped.");
//									}
//									continue;
//								}
//								
//								if(dbUtil.isAlreadyIndexing(platformDBConnection, sliceID, indexName)) {
//									String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
//											append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
//									logger.info(msg);
//									System.out.println(msg);
//								} else {
//									dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Incremental);
//									
//									long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(sliceID, indexName);
//									
//									String msg = new StringBuffer("Initiating task for index: ").append(indexName).append(", last time indexed on: ").append(lLastIndexRunTime).toString(); 
//									logger.info(msg);
//									System.out.println(msg);
//									
//									Index index = indexMap.get(indexName);
//									String json = dbUtil.runQueryToJSON(sliceDBConn, index, lLastIndexRunTime, sliceID);
//									if(null != json) {
//										if(logger.isDebugEnabled()) {
//											logger.debug("==================== Incremental Index :: Data ====================");
//											logger.debug(json);
//										}
//									}
//									
//									JSONObject jsnobject = new JSONObject(json);
//									JSONArray jsonArray = jsnobject.getJSONArray("results");
//									bulkIndex(jsonArray, index);
//									
//									dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Completed);
//								}
//							} catch (Exception ex) {
//								ex.printStackTrace();
//								logger.error(ex.getMessage());
//								dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
//							}
//						}				
//					}
//				} catch (Exception ex) {
//					ex.printStackTrace();
//					logger.error(ex.getMessage());
//				} finally {
//					createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
//					dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//				}
//			}			
//		} catch (Exception ex) {
//			logger.error(ex.getMessage());
//		} finally {
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}		
//	}
//
//	private void doIncrementalIndexForSingleSlice(int sliceID) throws SQLException {
//		Connection platformDBConnection = null;
//		Connection sliceDBConn = null;
//		
//		try {
//			SliceIndexMetadata slicesIndexMetadata = dbUtil.getSlicesIndexMetaData();
//			
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			HashMap<Integer, SliceDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
//			sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
//		
//			if(null != sliceDBConn) {
//				boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigESSearchEnabledForSlice(sliceDBConn, sliceID, false);
//				if(!bIsSliceConfigESSearchEnabled) {
//					String msg = "Slice config <ENABLE_ELASTIC_SEARCH> is not turned on for slice: " + sliceID +". Skipping indexing...";
//					System.out.println(msg);
//					logger.warn(msg);
//					return;
//				}
//				
//				System.out.println("Connected to slice: " + sliceID);
//				logger.info("Incremental Indexing, connected to slice: " + sliceID);
//				
//				for (String indexName : indexKeys) {
//					try {
//						if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
//							if(logger.isDebugEnabled()) {
//								logger.debug(indexName + " not set for indexing..so skipped.");
//							}
//							continue;
//						}
//						
//						if(dbUtil.isAlreadyIndexing(platformDBConnection, sliceID, indexName)) {
//							String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
//									append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
//							logger.info(msg);
//							System.out.println(msg);
//						} else {
//							dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Incremental);
//							
//							long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(sliceID, indexName);
//							
//							Index index = indexMap.get(indexName);
//							String json = dbUtil.runQueryToJSON(sliceDBConn, index, lLastIndexRunTime, sliceID);
//							if(null != json) {
//								if(logger.isDebugEnabled()) {
//									logger.debug("==================== Incremental Index :: Data ====================");
//									logger.debug(json);
//								}
//							}
//							
//							JSONObject jsnobject = new JSONObject(json);
//							JSONArray jsonArray = jsnobject.getJSONArray("results");
//							bulkIndex(jsonArray, index);
//							
//							dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Completed);
//						}
//					} catch (Exception ex) {
//						ex.printStackTrace();
//						logger.error(ex.getMessage());
//						dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
//					}
//				}
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
//			dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}			
//	}
//	
//	private void doIncrementalIndexForAllSlicesThreaded() {
//		Connection platformDBConnection = null;
//		
//		try {
//			SliceIndexMetadata slicesIndexMetadata = dbUtil.getSlicesIndexMetaData();
//			
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			HashMap<Integer, SliceDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
//			Set<Integer> sliceKeys = sliceDBMap.keySet();
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
//			for (Integer sliceID : sliceKeys) {
//				Connection sliceDBConn = null;
//				try {
//					sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//					if(null != sliceDBConn) {
//						boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigESSearchEnabledForSlice(sliceDBConn, sliceID, false);
//						if(!bIsSliceConfigESSearchEnabled) {
//							String msg = "Slice config <ENABLE_ELASTIC_SEARCH> is not turned on for slice: " + sliceID +". Skipping indexing...";
//							System.out.println(msg);
//							logger.warn(msg);
//							continue;
//						}
//						
//						System.out.println("Connected to slice: " + sliceID);
//						logger.info("Incremental Indexing, connected to slice: " + sliceID);
//						
//						for (String indexName : indexKeys) {
//							try {
//								if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
//									if(logger.isDebugEnabled()) {
//										logger.debug(indexName + " not set for indexing..so skipped.");
//									}
//									continue;
//								}
//								
//								if(dbUtil.isAlreadyIndexing(platformDBConnection, sliceID, indexName)) {
//									String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
//											append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
//									logger.info(msg);
//									System.out.println(msg);
//								} else {
//									dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Incremental);
//									
//									long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(sliceID, indexName);
//									
//									String msg = new StringBuffer("Initiating task for index: ").append(indexName).append(", last time indexed on: ").append(lLastIndexRunTime).toString(); 
//									logger.info(msg);
//									System.out.println(msg);
//									
//									Index index = indexMap.get(indexName);
//									ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
//									tim.indexData(index, sliceID, IndexOperationType.Incremental, lLastIndexRunTime);						
//								}
//							} catch (Exception ex) {
//								ex.printStackTrace();
//								logger.error(ex.getMessage());
//								dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
//							}
//						}				
//					}
//				} catch (Exception ex) {
//					ex.printStackTrace();
//					logger.error(ex.getMessage());
//				} finally {
//					createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
//					dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//				}
//			}			
//		} catch (Exception ex) {
//			logger.error(ex.getMessage());
//		} finally {
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}			
//	}
//	
//	private void doIncrementalIndexForSingleSliceThreaded(int sliceID) {
//		Connection platformDBConnection = null;
//		Connection sliceDBConn = null;
//		
//		try {
//			SliceIndexMetadata slicesIndexMetadata = dbUtil.getSlicesIndexMetaData();
//			
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			HashMap<Integer, SliceDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
//			sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
//		
//			if(null != sliceDBConn) {
//				boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigESSearchEnabledForSlice(sliceDBConn, sliceID, false);
//				if(!bIsSliceConfigESSearchEnabled) {
//					String msg = "Slice config <ENABLE_ELASTIC_SEARCH> is not turned on for slice: " + sliceID +". Skipping indexing...";
//					System.out.println(msg);
//					logger.warn(msg);
//					return;
//				}
//				
//				System.out.println("Connected to slice: " + sliceID);
//				logger.info("Incremental Indexing, connected to slice: " + sliceID);
//				
//				for (String indexName : indexKeys) {
//					try {
//						if(!isValidDocumentToIndex(indexDocumentType, indexName)) {
//							if(logger.isDebugEnabled()) {
//								logger.debug(indexName + " not set for indexing..so skipped.");
//							}
//							continue;
//						}
//						
//						if(dbUtil.isAlreadyIndexing(platformDBConnection, sliceID, indexName)) {
//							String msg = new StringBuffer("WARNING: Skipping, as an indexing operation is already in progress for index <").
//									append(indexName).append(">, for slice: <").append(sliceID).append(">.").toString();
//							logger.info(msg);
//							System.out.println(msg);
//						} else {
//							dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Incremental);
//							
//							long lLastIndexRunTime = slicesIndexMetadata.getLastIndexRunTime(sliceID, indexName);
//							
//							Index index = indexMap.get(indexName);
//							ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
//							tim.indexData(index, sliceID, IndexOperationType.Incremental, lLastIndexRunTime);						
//						}
//					} catch (Exception ex) {
//						ex.printStackTrace();
//						logger.error(ex.getMessage());
//						dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
//					}
//				}
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
//			dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}				
//	}
//
//	@Override
//	public boolean indexNearRealTime() {
//		// TODO Auto-generated method stub
//		return false;
//	}
//}
