package org.nextlevel.es.config;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nextlevel.es.IndexOperationType;
import org.nextlevel.es.IndexStatus;
import org.nextlevel.es.SliceIndexMetadata;
import org.nextlevel.es.SliceIndexMetadataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.tc.TransportClientProvider;
import org.nextlevel.es.tenants.Tenant;
import org.nextlevel.es.tenants.TenantsConfiguration;
import org.nextlevel.es.tenants.TenantsConfigurationReader;

public class IndexUtil {
	private static Logger logger = LoggerFactory.getLogger(IndexUtil.class);
	private TransportClientProvider tcp = TransportClientProvider.getInstance();
	private IndexConfigReader indexConfigReader = IndexConfigReader.getInstance();
	private TenantsConfiguration tenantsConfig = TenantsConfigurationReader.getInstance().getConfiguration();

	public static final String TENANTS_METADATA_INDEX = "TenantsMetadata";
	private static final int ES_RESULT_SIZE = 9999;
	
	public SliceIndexMetadata getSlicesIndexMetaData() {
		SliceIndexMetadata indexMetadataForSlices = new SliceIndexMetadata();
		HashMap<String, HashMap<String, SliceIndexMetadataRecord>> mapIndexDataForSlices = new HashMap<String, HashMap<String, SliceIndexMetadataRecord>>();
		indexMetadataForSlices.setMapIndexDataForSlices(mapIndexDataForSlices);
		try {
			Client esClient = tcp.getTransportClient();
			SearchRequestBuilder srb = esClient.prepareSearch(indexConfigReader.getIndexConfiguration().getParentIndexName())
					   .setTypes(TENANTS_METADATA_INDEX)
					   .setSize(ES_RESULT_SIZE)
					   .setSearchType(SearchType.QUERY_AND_FETCH)
					   .setQuery(QueryBuilders.matchAllQuery());

			SearchResponse resp = srb.execute().actionGet();
			SearchHits hits = resp.getHits();
			if(null != hits && hits.getTotalHits() > 0) {
				for (SearchHit searchHit : hits) {
					Map<String, Object> source = searchHit.getSource();
					
					String sliceID = (String) getData(source, "tenant");
					String indexDocType = (String) getData(source, "document_type");
					int lastIndxedOn = (int) getData(source, "last_indexed_at");
					String indexStatus = (String) getData(source, "status");
					String lastIndexType = (String) getData(source, "last_index_type");
					int lIndexStartedAt = (int) getData(source, "execution_started_at");
					String appId = (String) getData(source, "appid");
					HashMap<String, SliceIndexMetadataRecord> mapIndexDataForSlice = mapIndexDataForSlices.get(sliceID);
					if (null == mapIndexDataForSlice) {
						mapIndexDataForSlice = new HashMap<String, SliceIndexMetadataRecord>();
						mapIndexDataForSlices.put(sliceID, mapIndexDataForSlice);
					}
					SliceIndexMetadataRecord record = new SliceIndexMetadataRecord(sliceID, indexDocType, lastIndxedOn,
							indexStatus, lastIndexType, lIndexStartedAt, appId);
					mapIndexDataForSlice.put(indexDocType, record);
					if (logger.isDebugEnabled()) {
						logger.debug(record.toString());
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return indexMetadataForSlices;
	}

//	public SliceIndexMetadata getSliceIndexMetaData(int sliceID) {
//		SliceIndexMetadata indexMetadataForSlices = new SliceIndexMetadata();
//		HashMap<Integer, HashMap<String, SliceIndexMetadataRecord>> mapIndexDataForSlices = new HashMap<Integer, HashMap<String, SliceIndexMetadataRecord>>();
//		indexMetadataForSlices.setMapIndexDataForSlices(mapIndexDataForSlices);
//		PreparedStatement stmt = null;
//		ResultSet rsIndex = null;
//		Connection platformDBConnection = getPlatformSecondaryDBConnection();
//		try {
//			String fetchIndexStatusSql = "SELECT INDEX_DOCUMENT_TYPE, LAST_INDEXED_ON, INDEX_STATUS, LAST_INDEX_TYPE, EXECUTION_STARTED_AT FROM ES_INDEX_STATUS"
//					+ " WHERE SLICE=? ORDER BY SLICE";
//			stmt = platformDBConnection.prepareStatement(fetchIndexStatusSql);
//			stmt.setInt(1, sliceID);
//			logger.info("Executing query: " + fetchIndexStatusSql);
//
//			rsIndex = stmt.executeQuery();
//			while (rsIndex.next()) {
//				String indexDocType = rsIndex.getString(1);
//				long lastIndxedOn = rsIndex.getLong(2);
//				String indexStatus = rsIndex.getString(3);
//				String lastIndexType = rsIndex.getString(4);
//				long lIndexStartedAt = rsIndex.getLong(5);
//				HashMap<String, SliceIndexMetadataRecord> mapIndexDataForSlice = mapIndexDataForSlices.get(sliceID);
//				if (null == mapIndexDataForSlice) {
//					mapIndexDataForSlice = new HashMap<String, SliceIndexMetadataRecord>();
//					mapIndexDataForSlices.put(sliceID, mapIndexDataForSlice);
//				}
//
//				SliceIndexMetadataRecord record = new SliceIndexMetadataRecord(sliceID, indexDocType, lastIndxedOn,
//						indexStatus, lastIndexType, lIndexStartedAt);
//				mapIndexDataForSlice.put(indexDocType, record);
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if (null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//
//			if (null != rsIndex) {
//				try {
//					rsIndex.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//
//			closeDBConnection(platformDBConnection, DBType.PlatformDBSecondary);
//		}
//
//		return indexMetadataForSlices;
//	}
	
	private Object getData(Map<String, Object> source, String key) {
		return source.get(key);
	}

	public void updateTenantMetadata(SliceIndexMetadataRecord metadataRecord, SearchHit searchHit) {
		try {
			IndexConfig indexConfig = indexConfigReader.getIndexConfiguration();
			Client esClient = tcp.getTransportClient();
			Map<String, Object> source = null;
			String id = null;
			if(null != searchHit) {
				id = searchHit.getId();
				source = searchHit.getSource();	
				source.put("status", metadataRecord.getIndexStatus());
				source.put("last_indexed_at", metadataRecord.getlLastIndexRunTime());
				source.put("last_index_type", metadataRecord.getLastIndexType());
			} else {
				Tenant tenant = tenantsConfig.getTenant(metadataRecord.getSliceID());
				id = metadataRecord.getSliceID() + "_" + metadataRecord.getDocumentType();
				
				source = new HashMap<String, Object>();
				source.put("tenant", metadataRecord.getSliceID());
				source.put("status", metadataRecord.getIndexStatus());
				source.put("last_indexed_at", metadataRecord.getlLastIndexRunTime());
				source.put("last_index_type", metadataRecord.getLastIndexType());
				source.put("execution_started_at", metadataRecord.getlIndexStartedAt());
				source.put("document_type", metadataRecord.getDocumentType());
				source.put("appid", tenant.getAppId());
			}
			
			IndexResponse response = esClient.prepareIndex(indexConfig.getParentIndexName(), TENANTS_METADATA_INDEX, id)
		        .setSource(source)
		        .get();
			StringBuffer sbMessage = new StringBuffer("Tenant index metadata ");
			if(searchHit != null){
				sbMessage.append("updated: ");
			} else {
				sbMessage.append("inserted :");
			}
			
			sbMessage.append(response.status().getStatus())
					.append(" , Tenant ID <")
					.append(metadataRecord.getSliceID())
					.append(">.")
					.toString();
			System.out.println(sbMessage.toString());
			logger.info(sbMessage.toString());
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		}
	}

	public boolean isAlreadyIndexing(String sliceID, String indexName) {
		try {
			IndexConfig indexConfig = indexConfigReader.getIndexConfiguration();
			Client esClient = tcp.getTransportClient();
			
			BoolQueryBuilder bqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery("tenant", sliceID))
					.must(QueryBuilders.termQuery("documentType", indexName))
					.must(QueryBuilders.termQuery("status", IndexStatus.Running.getValue()));

			SearchRequestBuilder srb = esClient.prepareSearch(indexConfig.getParentIndexName())
					   .setTypes(TENANTS_METADATA_INDEX)
					   .setSize(1)
					   .setSearchType(SearchType.QUERY_AND_FETCH)
					   .setQuery(bqb);
			SearchResponse resp = srb.execute().actionGet();
			SearchHits hits = resp.getHits();
			if(null != hits && hits.getTotalHits() > 0) {
				return true;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		}
		
		return false;
	}
	
	/**
	 * When it picks up an index for creation, some of the status is updated
	 * @param sliceID - for which slice
	 * @param indexDocType - index document type (like - Ticket, KBArticle, Contact, etc.)
	 * @throws SQLException 
	 */
	public void updateIndexStatus(String sliceID, String indexDocType, IndexOperationType opType, IndexStatus indexStatus) {
		try {
			long currentDateTime = System.currentTimeMillis();
			currentDateTime = currentDateTime/1000;
			SliceIndexMetadataRecord metaRecord = new SliceIndexMetadataRecord(sliceID, indexDocType, currentDateTime, indexStatus.getValue(), opType.getValue(), currentDateTime, tenantsConfig.getTenant(sliceID).getAppId());
			SearchHit indexMetadataRecord = isIndexStatusExists(sliceID, indexDocType);
			updateTenantMetadata(metaRecord, indexMetadataRecord);
		} catch (Exception ex) {
			ex.printStackTrace();
		}		
	}
	
	private SearchHit isIndexStatusExists(String sliceID, String indexDocType) {
		try {
			IndexConfig indexConfig = indexConfigReader.getIndexConfiguration();
			Client esClient = tcp.getTransportClient();
			BoolQueryBuilder bqb = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery("tenant", sliceID))
					.must(QueryBuilders.termQuery("document_type", indexDocType));

			SearchRequestBuilder srb = esClient.prepareSearch(indexConfig.getParentIndexName())
					   .setTypes(TENANTS_METADATA_INDEX)
					   .setSize(1)
					   .setSearchType(SearchType.QUERY_AND_FETCH)
					   .setQuery(bqb);
			SearchResponse resp = srb.execute().actionGet();
			SearchHits hits = resp.getHits();
			if(null != hits && hits.getTotalHits() == 1) {
				return hits.getAt(0);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return null;
	}
	

}
