package org.nextlevel.es;
//package org.nextlevel.es;
//
//import java.sql.Connection;
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
//import org.nextlevel.es.db.util.DBType;
//import org.nextlevel.es.db.util.DatabaseUtil;
//import org.nextlevel.es.db.util.TenantDBConnectionData;
//
///**
// * 
// * @author nextlevel
// *
// */
//public final class FullIndexManager2 extends IndexManager {
//	private static Logger logger = LoggerFactory.getLogger(FullIndexManager2.class);
//	
//	private static HashMap<String, TenantDBConnectionData> sliceDBMap;
//	
//	public FullIndexManager2() {
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
//		try {
//			//if slice is not set, it is for all the slices
//			if(sliceID < 1) {
//				if(isMultiThreadingEnabled) {
//					System.out.println("Doing threaded indexing...");
//					long currentTime = System.currentTimeMillis();
//					
//					doFullIndexForAllSlicesThreaded();
//					
//					long endTime = System.currentTimeMillis();
//					long timeTaken = (endTime-currentTime)/(1000*60);
//					System.out.println("TIME TAKEN (MIN): " + timeTaken);
//					logger.info("TIME TAKEN (MIN): " + timeTaken);
//				} else {
//					doFullIndexForAllSlices();
//				}
//			} else { //slice specific indexing
//				if(isMultiThreadingEnabled) {
//					System.out.println("Doing threaded indexing ...");
//					
//					doFullIndexForSingleSliceThreaded(sliceID);
//				} else {
//					doFullIndexForSingleSlice(sliceID);
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
//	//TODO::Multi-threaded per slice
//	private void doFullIndexForAllSlices() {
//		Connection platformDBConnection = null;
//		try {
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			sliceDBMap = dbUtil.getSliceDBsDetails();
//			Set<String> sliceKeys = sliceDBMap.keySet();
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
//			for (String sliceID : sliceKeys) {
//				//First delete all documents from the index - before doing full indexing
//				deleteAllIndexDataForSlice(sliceID, indexDocumentType);
//				
//				//Now do full indexing
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
//						logger.info("Full Indexing, connected to slice: " + sliceID);
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
//									dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Full);
//									
//									Index index = indexMap.get(indexName);
//									String json = dbUtil.runQueryToJSON(sliceDBConn, index.getQuery(), sliceID, index);
//									
//									if(logger.isDebugEnabled()) {
//										if(null != json) {
//											logger.debug("==================== Full Index :: Data ====================");
//											logger.debug(json);
//										}
//									}
//									
//									JSONObject jsnobject = new JSONObject(json);
//									JSONArray jsonArray = jsnobject.getJSONArray("results");
//									bulkIndex(jsonArray, index);
//									dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Completed);
//								}
//							} catch (Exception ex) {
//								ex.printStackTrace();
//								logger.error(ex.getMessage());
//								dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
//							}
//						}				
//					}
//				} finally {
//					createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
//					dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//				}
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}
//	}
//
//	//TODO::Refactoring of common code between doFullIndexForAllSlices, doFullIndexForSingleSlice
//	private void doFullIndexForSingleSlice(int sliceID) {
//		//First delete all documents from the index - before doing full indexing
//		deleteAllIndexDataForSlice(sliceID, indexDocumentType);
//		
//		//Now do full indexing
//		Connection sliceDBConn = null;		
//		Connection platformDBConnection = null;
//		try {
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			sliceDBMap = dbUtil.getSliceDBsDetails();
//			sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
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
//				logger.info("Full Indexing, connected to slice: " + sliceID);
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
//							dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Full);
//							
//							Index index = indexMap.get(indexName);
//							String json = dbUtil.runQueryToJSON(sliceDBConn, index.getQuery(), sliceID, index);
//							if(logger.isDebugEnabled()) {
//								if(null != json) {
//									logger.debug("==================== Full Index :: Data ====================");
//									logger.debug(json);
//								}
//							}
//							
//							JSONObject jsnobject = new JSONObject(json);
//							JSONArray jsonArray = jsnobject.getJSONArray("results");
//							bulkIndex(jsonArray, index);
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
//	
//	private void doFullIndexForAllSlicesThreaded() {
//		Connection platformDBConnection = null;
//		try {
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			sliceDBMap = dbUtil.getSliceDBsDetails();
//			Set<Integer> sliceKeys = sliceDBMap.keySet();
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
//			for (Integer sliceID : sliceKeys) {
//				//First delete all documents from the index - before doing full indexing
//				deleteAllIndexDataForSlice(sliceID, indexDocumentType);
//				
//				//Now do full indexing
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
//						logger.info("Full Indexing, connected to slice: " + sliceID);
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
//									dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Full);
//									
//									Index index = indexMap.get(indexName);
//									ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
//									tim.indexData(index, sliceID, IndexOperationType.Full, 0);
//								}
//							} catch (Exception ex) {
//								ex.printStackTrace();
//								logger.error(ex.getMessage());
//								dbUtil.updateIndexCompletionStatus(sliceID, indexName, IndexStatus.Failed);
//							}
//						}				
//					}
//				} finally {
//					createAlias(sliceID, indexConfigReader.getIndexConfiguration().getParentIndexName());
//					dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//				}
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			dbUtil.closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}
//	}
//
//	//TODO::Refactoring of common code between doFullIndexForAllSlices, doFullIndexForSingleSlice
//	private void doFullIndexForSingleSliceThreaded (int sliceID) {
//		//First delete all documents from the index - before doing full indexing
//		deleteAllIndexDataForSlice(sliceID, indexDocumentType);
//		
//		//Now do full index
//		Connection sliceDBConn = null;		
//		Connection platformDBConnection = null;
//		try {
//			IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			Set<String> indexKeys = indexMap.keySet(); 
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			sliceDBMap = dbUtil.getSliceDBsDetails();
//			sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//			platformDBConnection = dbUtil.getPlatformSecondaryDBConnection();
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
//				logger.info("Full Indexing, connected to slice: " + sliceID);
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
//							dbUtil.updateIndexInitiatedStatus(sliceID, indexName, IndexOperationType.Full);
//							
//							Index index = indexMap.get(indexName);
//							ThreadedIndexManager tim = ThreadedIndexManager.getInstance();
//							tim.indexData(index, sliceID, IndexOperationType.Full, 0);							
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
