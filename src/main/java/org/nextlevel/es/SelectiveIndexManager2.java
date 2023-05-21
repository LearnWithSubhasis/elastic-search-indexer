package org.nextlevel.es;
//package org.nextlevel.es;
//
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Set;
//
//import org.json.JSONArray;
//import org.json.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.nextlevel.es.config.Index;
//import org.nextlevel.es.config.IndexConfig;
//import org.nextlevel.es.db.util2.DBType;
//import org.nextlevel.es.db.util2.DatabaseUtil;
//import org.nextlevel.es.db.util2.SliceDBConnectionData;
//
///**
// * 
// * @author nextlevel
// *
// */
//public final class SelectiveIndexManager2 extends IndexManager {
//	private static Logger logger = LoggerFactory.getLogger(SelectiveIndexManager2.class);
//
//	public SelectiveIndexManager2() {
//		super();
//		try {
//			esClient = tcp.getTransportClient();
//		} catch (Exception e) {
//			e.printStackTrace();
//			logger.error(e.getMessage());
//			System.out.println(e.getMessage());
//		}		
//	}
//
//	@Override
//	public boolean index() {
//		try {
//			//Selective index is meant for single slice
//			if(null != selectedIDsToIndex && selectedIDsToIndex.size() >= 1 && sliceID > 1 && indexDocumentType != null) {
//				doSelectiveIndexForASlice(); 
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		
//		return false;
//	}
//
//	private void doSelectiveIndexForASlice() throws SQLException {
//		Connection sliceDBConn = null;
//	
//		try {
//			HashMap<String, Index> indexMap = indexConfigReader.getIndexMap();
//			
//			DatabaseUtil dbUtil = new DatabaseUtil();
//			HashMap<Integer, SliceDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();
//			sliceDBConn = dbUtil.getSliceSecondaryDBConnection(sliceDBMap.get(sliceID));
//			if(null != sliceDBConn) {
//				boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigESSearchEnabledForSlice(sliceDBConn, sliceID, false);
//				if(!bIsSliceConfigESSearchEnabled) {
//					String msg = "Slice config <ENABLE_ELASTIC_SEARCH> is not turned on for slice: " + sliceID +". Skipping indexing...";
//					System.out.println(msg);
//					logger.warn(msg);
//					return;
//				}
//				
//				System.out.println("Fetching data, connected to slice: " + sliceID);
//				
//				Index index = indexMap.get(indexDocumentType);
//				if(null != index) {
//					switch(indexOperationMethod) {
//					case Delete:
//							deleteSelectedRecordsFromIndex(indexConfigReader.getParentIndexName(index));
//							break;
//						
//					default:					
//				
//						String json = dbUtil.runQueryToJSON(sliceDBConn, index, selectedIDsToIndex, sliceID);
//						if(null != json) {
//							logger.info("============== Data for Selective Indexing ==============");
//							logger.info(json);						
//						} else {
//							logger.error("**************Null converted data !!!!!!!!!!!!");
//							System.out.println("*************Null converted data !!!!!!!!!!!!");
//						}
//	//					if(logger.isDebugEnabled()) {
//	//						if(null != json) {
//	//							logger.debug("============== Data for Selective Indexing ==============");
//	//							logger.debug(json);
//	//						}
//	//					}
//						
//						JSONObject jsnobject = new JSONObject(json);
//						JSONArray jsonArray = jsnobject.getJSONArray("results");
//						for(int i=0; i<jsonArray.length(); i++) {
//							JSONObject jsonDataObj = (JSONObject) jsonArray.get(i);
//							System.out.println("Index record:"+ jsonDataObj.toString());
//							logger.info("Index record:"+ jsonDataObj.toString());
//							
//							indexData(index, jsonDataObj, sliceID);
//						}
//					}
//				}
//			} 
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			dbUtil.closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//		}			
//	}
//
//	
//	public boolean performNearRealtimeIndex() {
//		boolean bReturn = true;
//		DatabaseUtil dbUtil = new DatabaseUtil();
//		SliceIndexMetadata metadata = dbUtil.getSlicesIndexMetaData();	
//		HashMap<Integer, SliceDBConnectionData> sliceDBMap = dbUtil.getSliceDBsDetails();	
//		
//		HashMap<Integer, HashMap<String, SliceIndexMetadataRecord>> metadataRecords = metadata.getMapIndexDataForSlices();
//		Set<Integer> sliceKeys = metadataRecords.keySet();
//		
//		for (Integer sliceID : sliceKeys) {
//			try {
//				logger.info("NEAR REALTIME INDEXING:: slice: " + sliceID);
//				
//				HashMap<String, SliceIndexMetadataRecord> metadataPerSlice = metadataRecords.get(sliceID);
//				Set<String> docKeys = metadataPerSlice.keySet();
//				HashMap<String, Boolean> validDocMap = new HashMap<String, Boolean>();
//				boolean bSliceApplicableForIndexing = false;
//				for (String indexDocType : docKeys) {
//					SliceIndexMetadataRecord record = metadataPerSlice.get(indexDocType);
//					if((record.getLastIndexType() != null) && 
//							(record.getIndexStatus() != null && record.getIndexStatus().equalsIgnoreCase(IndexStatus.Completed.getValue()))) {
//						bSliceApplicableForIndexing = true;
//						validDocMap.put(indexDocType.toLowerCase(), true);
//					}
//				}
//				
//				if(!bSliceApplicableForIndexing) {
//					//if (logger.isDebugEnabled()) {
//						logger.info("Slice " + sliceID + " is not applicable for realtime indexing. Please see if this slice has been indexed (full/incremental) at least once completely.");
//					//}
//					
//					continue;
//				}				
//				
//				String msg;
//				
//				//TODO::This should borrow connection from secondary database
//				SliceDBConnectionData sliceDBConnData = sliceDBMap.get(sliceID);
//				if(null != sliceDBConnData) {
//					boolean bSliceDeprovisioned = false;
//					HashMap<String, List<SelectiveIndexData>> dataToIndex = dbUtil.getSelectiveIndexDataRecords(sliceDBConnData, sliceID, indexConfigReader.getIndexConfiguration().isDeleteRecordsAfterIndexing());
//					Set<String> documentKeys = dataToIndex.keySet();
//					for (String docType : documentKeys) {
//						logger.info("NEAR REALTIME INDEXING:: Loop for document type: " + docType);
//						
//						boolean bBulkDelete = false, bBulkUpdate = false;
//						if(validDocMap.containsKey(docType.toLowerCase())) {
//							logger.info("NEAR REALTIME INDEXING:: Selected valid document type: " + docType);
//							List<SelectiveIndexData> listRecords = dataToIndex.get(docType);
//							ArrayList<Long> itemsToUpdate = new ArrayList<Long>();
//							ArrayList<Long> itemsToDelete = new ArrayList<Long>();
//							
//							for (SelectiveIndexData selectiveIndexData : listRecords) {
//								logger.info("NEAR REALTIME INDEXING:: Initiating index for record: " + selectiveIndexData);
//								
//								ListenerEventType eventType = selectiveIndexData.getEventType();
//								switch(eventType) {
//									case CREATE:
//									case UPDATE:
//										logger.info("NEAR REALTIME INDEXING:: event type: " + eventType);
//										itemsToUpdate.add(selectiveIndexData.getItemID());
//										break;
//										
//									case DELETE:
//										logger.info("NEAR REALTIME INDEXING:: event type: " + eventType);
//										itemsToDelete.add(selectiveIndexData.getItemID());
//										break;
//										
//									case BULKUPDATE:
//										logger.info("NEAR REALTIME INDEXING:: event type: " + eventType);
//										bBulkUpdate = true;
//										break;
//										
//									case BULKDELETE:
//										logger.info("NEAR REALTIME INDEXING:: event type: " + eventType);
//										bBulkDelete = true;
//										break;
//										
//									case SLICE_DEPROVISION:
//										logger.info("NEAR REALTIME INDEXING:: event type: " + eventType);
//										bSliceDeprovisioned = true;
//										break;
//										
//									default:
//										logger.info("NEAR REALTIME INDEXING:: event type: " + eventType);
//										break;
//								}
//								
//								if(bBulkUpdate || bBulkDelete || bSliceDeprovisioned) {
//									logger.info("NEAR REALTIME INDEXING:: Bulk update/depete or slice deprovision event found. Breaking out of loop.");
//									break;
//								}
//							}
//							
//							
//							if((bBulkUpdate || bBulkDelete) && !bSliceDeprovisioned) {
//								//TODO::mark the slice-document combination for delete
//								if(bBulkDelete) {
//									msg = "As bulk delete has been performed for slice <" + sliceID + ">, document type <" + docType + ">, marking the corresponding index for deletion.";
//									System.out.println(msg);
//									logger.info(msg);
//									deleteAllIndexDataForSlice(sliceID, docType);
//								} else if (bBulkUpdate) {
//									msg = "As bulk update has been performed for slice <" + sliceID + ">, document type <" + docType + ">, nothing to do. Next indexing will pick up updated records.";
//									System.out.println(msg);
//									logger.info(msg);
//								}
//								continue;
//							} else if (bSliceDeprovisioned) {
//								break;
//							}
//							
//							bReturn = createOrUpdateIndexForItems(sliceID, docType, itemsToUpdate);
//							bReturn = bReturn & deleteIndexForItems(sliceID, docType, itemsToDelete);
//						}
//					}
//					
//					if (bSliceDeprovisioned) {
//						//TODO::mark the slice for delete
//						msg = "As slice <" + sliceID + "> has been deprovisioned, marking the corresponding indexes for deletion.";
//						System.out.println(msg);
//						logger.info(msg);
//
//						deleteAllIndexDataForSlice(sliceID);
//						continue;
//					}					
//				}
//			} catch (Exception ex) {
//				bReturn = false;
//				logger.error(ex.getMessage());
//			}
//		}
//		
//		return bReturn;
//	}
//
//	private boolean deleteIndexForItems(Integer sliceID, String docType,
//			ArrayList<Long> itemsToDelete) {
//		
//		for (Long id : itemsToDelete) {
//			StringBuffer sbData = new StringBuffer("NEAR REALTIME INDEXING:: Sending for indexing (delete): Slice:").append(sliceID)
//					.append(", Document type: ").append(docType).append(", ID: ").append(id);
//			logger.info(sbData.toString());
//			System.out.println(sbData);
//		}
//		
//		IndexManager sm = new SelectiveIndexManager2();
//		sm.setSelectedIDsToIndex(itemsToDelete);
//		sm.setIndexDocumentType(docType);
//		sm.setIndexOperationMethod(IndexOperationMethod.Delete);
//		sm.setSliceID(sliceID);
//		return sm.index();
//	}
//
//	private boolean createOrUpdateIndexForItems(Integer sliceID, String docType,
//			ArrayList<Long> itemsToUpdate) {
//		
//		for (Long id : itemsToUpdate) {
//			StringBuffer sbData = new StringBuffer("NEAR REALTIME INDEXING:: Sending for indexing (create/update): Slice:").append(sliceID)
//					.append(", Document type: ").append(docType).append(", ID: ").append(id);
//			logger.info(sbData.toString());
//			System.out.println(sbData);
//		}
//		
//		IndexManager sm = new SelectiveIndexManager2();
//		sm.setSelectedIDsToIndex(itemsToUpdate);
//		sm.setIndexDocumentType(docType);
//		sm.setIndexOperationMethod(IndexOperationMethod.Create);
//		sm.setSliceID(sliceID);
//		return sm.index();		
//	}
//	
//	/**
//	 * This will delete for the given slice, for all document types
//	 * @param sliceID
//	 * @return
//	 */
//	private boolean deleteAllIndexDataForSlice(Integer sliceID) {
//		StringBuffer docTypes = new StringBuffer();
//		IndexConfig configData = indexConfigReader.getIndexConfiguration();
//		ArrayList<Index> indexes = configData.getIndexes();
//		int i = 0;
//		for (Index index : indexes) {
//			i++;
//			docTypes.append(index.getDocumentType());
//			if(i < indexes.size()) {
//				docTypes.append(",");
//			}				
//		}
//		
//		return deleteAllIndexDataForSlice(sliceID, docTypes.toString());
//	}
//	
//	@Override
//	public boolean indexNearRealTime() {
//		return performNearRealtimeIndex();
//	}
//}
