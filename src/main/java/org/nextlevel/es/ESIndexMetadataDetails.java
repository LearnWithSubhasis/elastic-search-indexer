package org.nextlevel.es;
//package org.nextlevel.es;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Set;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.nextlevel.es.config.Index;
//import org.nextlevel.es.config.IndexConfig;
//import org.nextlevel.es.config.IndexConfigReader;
//import org.nextlevel.es.db.util.DatabaseUtil;
//import org.nextlevel.es.db.util.TenantDBConnectionData;
//
///**
// * The indexing services in admin-services component interface thru this implementation.
// * 
// * @author nextlevel
// *
// */
//public final class ESIndexMetadataDetails {
//	private static Logger logger = LoggerFactory.getLogger(IndexClient.class);
//	
//	private DatabaseUtil dbUtil = new DatabaseUtil();
//	
//	public ArrayList<ESIndexMetadata> getIndexDetails() {
//		ArrayList<ESIndexMetadata> listMetadata = new ArrayList<ESIndexMetadata>();
//		
//		IndexConfigReader configReader = IndexConfigReader.getInstance();
//		IndexConfig indexConfig = configReader.getIndexConfiguration();
//		ArrayList<Index> indexesPossible = indexConfig.getIndexes();
//		
//		SliceIndexMetadata actualIndexMetadata = dbUtil.getSlicesIndexMetaData();
//		
//		HashMap<Integer, SliceDBConnectionData> mapSliceDB = dbUtil.getSliceDBsDetails();
//		Set<Integer> sliceKeys = mapSliceDB.keySet();
//		for (Integer slice : sliceKeys) {
//			SliceDBConnectionData sliceConn = mapSliceDB.get(slice);
//			boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigForElasticSearchEnabled(sliceConn);
//			fetchIndexMetadata(actualIndexMetadata, slice, indexesPossible, bIsSliceConfigESSearchEnabled, sliceConn.getDbSchemaName(), "any", listMetadata);
//		}
//		
//		return listMetadata;
//	}
//
//	public ArrayList<ESIndexMetadata> getIndexDetails(int sliceParam, String isConfigEnabled, String indexStatus) {
//		String msg = new StringBuffer("Fetching index details, Slice=").append(sliceParam).append(", isConfigEnabled=").
//				append(isConfigEnabled).append(", indexStatus=").append(indexStatus).toString();
//		logger.info(msg);
//		System.out.println(msg);
//		
//		if(sliceParam < 1 
//				&& (isConfigEnabled.equalsIgnoreCase("any") || isConfigEnabled.equalsIgnoreCase("all")) 
//				&& (indexStatus.equalsIgnoreCase("any") || indexStatus.equalsIgnoreCase("all"))) {
//			return getIndexDetails();
//		}
//		
//		boolean bOnlyConfigEnabled = Boolean.parseBoolean(isConfigEnabled);
//		
//		ArrayList<ESIndexMetadata> listMetadata = new ArrayList<ESIndexMetadata>();
//		
//		IndexConfigReader configReader = IndexConfigReader.getInstance();
//		IndexConfig indexConfig = configReader.getIndexConfiguration();
//		ArrayList<Index> indexesPossible = indexConfig.getIndexes();
//		
//		HashMap<Integer, SliceDBConnectionData> mapSliceDB = dbUtil.getSliceDBsDetails();
//		Set<Integer> sliceKeys = mapSliceDB.keySet();
//		for (Integer slice : sliceKeys) {
//			if(sliceParam < 1 || slice.intValue() == sliceParam) {			
//				SliceDBConnectionData sliceConn = mapSliceDB.get(slice);
//				SliceIndexMetadata actualIndexMetadata = dbUtil.getSliceIndexMetaData(slice);
//				
//				boolean bIsSliceConfigESSearchEnabled = dbUtil.isSliceConfigForElasticSearchEnabled(sliceConn);
//				if(bIsSliceConfigESSearchEnabled == bOnlyConfigEnabled) {				
//					fetchIndexMetadata(actualIndexMetadata, slice, indexesPossible, bIsSliceConfigESSearchEnabled, sliceConn.getDbSchemaName(), indexStatus, listMetadata);
//				} else if (isConfigEnabled.equalsIgnoreCase("any") || isConfigEnabled.equalsIgnoreCase("all")) {
//					fetchIndexMetadata(actualIndexMetadata, slice, indexesPossible, true, sliceConn.getDbSchemaName(), indexStatus, listMetadata);
//				}
//			}
//		}
//		
//		return listMetadata;
//	}
//
//	private void fetchIndexMetadata(SliceIndexMetadata actualIndexMetadata, Integer slice, ArrayList<Index> indexesPossible, 
//			boolean bIsSliceConfigESSearchEnabled, String sliceDBSchemaName, String indexStatus, ArrayList<ESIndexMetadata> listMetadata) {
//		HashMap<String, SliceIndexMetadataRecord> actualIndexMapForSlice = actualIndexMetadata.getMapIndexDataForSlice(slice);
//		for (Index index : indexesPossible) {
//			String indexDocumentType = index.getDocumentType();
//
//			SliceIndexMetadataRecord record = null;
//			if(null != actualIndexMapForSlice && null != indexDocumentType) {
//				record = actualIndexMapForSlice.get(indexDocumentType);
//			}
//			
//			String status = (null != record) ? record.getIndexStatus() : null;
//			
//			System.out.println(status+","+indexStatus);
//			
//			if(indexStatus.equalsIgnoreCase("any") || indexStatus.equalsIgnoreCase("all")) {
//				addToIndexList(record, listMetadata, slice, indexDocumentType, bIsSliceConfigESSearchEnabled, sliceDBSchemaName);
//			} else if(null != status && status.equalsIgnoreCase(indexStatus)) {
//				addToIndexList(record, listMetadata, slice, indexDocumentType, bIsSliceConfigESSearchEnabled, sliceDBSchemaName);
//			}
//		}
//		
//	}
//
//	private void addToIndexList(SliceIndexMetadataRecord record,
//			ArrayList<ESIndexMetadata> listMetadata, Integer slice, String indexDocumentType, 
//			boolean bIsSliceConfigESSearchEnabled, String sliceDBSchemaName) {
//		ESIndexMetadata indexMetadata = new ESIndexMetadata();
//		indexMetadata.setSlice(slice);
//		indexMetadata.setIndexDocumentType(indexDocumentType);
//		indexMetadata.setSliceConfigEnabled(bIsSliceConfigESSearchEnabled);
//		indexMetadata.setSliceDB(sliceDBSchemaName);
//		if(record != null) {
//			indexMetadata.setLastIndexedOn(record.getlLastIndexRunTime());
//			indexMetadata.setLastIndexOperationType(record.getLastIndexType());
//			indexMetadata.setLastIndexStartedAt(record.getlIndexStartedAt());
//			indexMetadata.setLastIndexStatus(record.getIndexStatus());
//		}
//		
//		listMetadata.add(indexMetadata);		
//	}
//	
//}
