package org.nextlevel.es.db.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.nextlevel.es.IndexOperationType;
import org.nextlevel.es.SelectiveIndexData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.config.Index;
import org.nextlevel.es.conn.ConnectionReader;
import org.nextlevel.es.conn.DefaultDatabase;
import org.nextlevel.es.tenants.Tenant;
import org.nextlevel.es.tenants.TenantsConfiguration;
import org.nextlevel.es.tenants.TenantsConfigurationReader;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * 
 * @author nextlevel
 *
 */
public class DatabaseUtil {
	private static Logger logger = LoggerFactory.getLogger(DatabaseUtil.class);	
	private ConnectionReader connectionReader = ConnectionReader.getInstance();
	private ResultsetToJsonConverter converter = ResultsetToJsonConverter.getInstance();
	private TenantsConfiguration tenantsConfig = TenantsConfigurationReader.getInstance().getConfiguration();
	private DBType dbType;
	private String databaseName;
	
	
	public void initializeSliceDBPools() {
		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();
		boolean bReturn = connFactory.initializeSliceDBPool();
		String msg = null;
		if(bReturn) {
			msg = "Initialized connection pool for slice databases.";
			System.out.println(msg);
			logger.info(msg);
		} else {
			msg = "ERROR: Couldn't initialize slice DB connection pool.";
			System.err.println(msg);
			logger.error(msg);
		}
	}

//	public void initializeSliceDBPools() {
//		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();
//		boolean bReturn = connFactory.initializeSliceDBPool(DBType.SliceDBSecondary);
//		String msg = null;
//		if(bReturn) {
//			msg = "Initialized connection pool for slice databases (Primary).";
//			System.out.println(msg);
//			logger.info(msg);
//		} else {
//			msg = "ERROR: Couldn't initialize slice DB connection pool (Primary).";
//			System.err.println(msg);
//			logger.error(msg);
//		}
//		
//		bReturn = connFactory.initializeSliceDBPool(DBType.SliceDBSecondary);
//		if(bReturn) {
//			msg = "Initialized connection pool for slice databases (Secondary).";
//			System.out.println(msg);
//			logger.info(msg);
//		} else {
//			msg = "ERROR: Couldn't initialize slice DB connection pool (Secondary).";
//			System.err.println(msg);
//			logger.error(msg);
//		}		
//	}
	
	public HashMap<String, TenantDBConnectionData> getSliceDBsDetails() {
		HashMap<String, TenantDBConnectionData> tenantDBMap = new HashMap<String, TenantDBConnectionData>();
		for (Tenant tenant : tenantsConfig.getAllTenants()) {
			TenantDBConnectionData data = TenantDBConnectionData.copyFrom(tenant, connectionReader.getConnectionData());
			tenantDBMap.put(tenant.getTenantId(), data);
			if(logger.isDebugEnabled()) {
				logger.debug("Adding to slice db map: " + data.toString());
			}
		}
		
		return tenantDBMap;
	}
	
	public Connection getTenantDBConnection(TenantDBConnectionData connData) {
		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();		
		connFactory.setTenantDBConnData(connData);
		
		DBConnectionPool connPool = connFactory.getAndInitializeDBConnectionPool(connData.getDbType());
		Connection tenantDBConn = null;
		try {
			tenantDBConn = connPool.getActualPool().borrowObject();//  getDataSource().getConnection();
			tenantDBConn.setCatalog(connData.getDbSchemaName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return tenantDBConn;
	}
	
	/**
	 * Basically used for Full Indexing (Non-batch version)
	 * @param sliceDBConn
	 * @param query
	 * @param sliceID
	 * @return
	 * @throws SQLException
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	public String runQueryToJSON (Connection sliceDBConn, String query, String sliceID, Index index) throws SQLException, JsonGenerationException, JsonMappingException, IOException {
		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
//		int count = StringUtils.countMatches(query, "?");
//
//		updateSliceCounter(query, stmt, sliceID, count);
		logger.info("Executing query: " + query);

		ResultSet rs = stmt.executeQuery();
		
		return converter.convert(rs, index);
	}
	
	public void closeDBConnection(Connection dbConn, DBType dbType) {
		returnConnection(dbConn, dbType);
	}
	
	public void returnConnection(Connection conn, DBType dbType) {
		this.dbType = dbType;
		
		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();
		connFactory.returnDBConnection(conn, dbType);

		// System.out.println("============ RELEASE::START ==========");
		// switch(dbType){
		// case PlatformDBPrimary:
		// System.out.println("pP: " + (--pP));
		// break;
		//
		// case PlatformDBSecondary:
		// System.out.println("pS: " + (--pS));
		// break;
		//
		// case SliceDBPrimary:
		// System.out.println("sP: " + (--sP));
		// break;
		//
		// case SliceDBSecondary:
		// System.out.println("sS: " + (--sS));
		// break;
		// }
		// System.out.println("============= RELEASE::END ===========");

	}
	
	/**
	 * Basically used for Full Indexing (Batch version)
	 * @param sliceDBConn
	 * @param query
	 * @param sliceID
	 * @param opType 
	 * @param currentEnd 
	 * @param currentStart 
	 * @return
	 * @throws SQLException
	 */
	public String runQueryToJSONInBatches (Connection sliceDBConn, String query, String sliceID, String modifiedDateCol, 
			int currentStart, int currentEnd, Index index) throws SQLException {
		String resultsetJson = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			query = returnBatchQuery(query, currentStart, currentEnd, modifiedDateCol);
			sliceDBConn.setCatalog(this.databaseName);
			
			stmt = sliceDBConn.prepareStatement(query);
//			int sliceCounter = StringUtils.countMatches(query, "?");
//			updateSliceCounter(query, stmt, sliceID, sliceCounter);
			logger.info("Executing query: " + query);

			resultsetJson = fetchResultAndTransform(query, stmt, sliceID, index, rs);		
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			closeStatement(stmt);
			closeResultset(rs);
			//closeDBConnection(sliceDBConn);
		}
		
		return resultsetJson;
	}

	private String returnBatchQuery(String query, int startIndex, int endIndex, String modifiedDateCol) {
		String queryTemp = query;//.substring(query.toLowerCase().indexOf("select") + 6);

		StringBuffer sbQuery = new StringBuffer(
				"SELECT T.* FROM ( ");
		sbQuery.append(queryTemp);
		sbQuery.append(") as T order by ").append(modifiedDateCol).append(" desc ");
		
		List<DefaultDatabase> defaultDatabases = ConnectionReader.getInstance().getConnectionData().getDefaultDatabases();
		if(null != defaultDatabases) {
			for (DefaultDatabase defaultDatabase : defaultDatabases) {
				if (null != defaultDatabase) {
					this.dbType = defaultDatabase.getDbType();
					this.databaseName = defaultDatabase.getDefaultTenantDBName();
					break;
				}
			}
		}
		
		if(null != this.dbType && this.dbType == DBType.MYSQL) {
			sbQuery.append("LIMIT ").append(endIndex-startIndex)
			.append(" OFFSET ").append(endIndex-startIndex);	
		} else {
			sbQuery.append("offset ").append(startIndex).append(" rows fetch next ")
			.append(endIndex-startIndex).append(" rows only");	
		}
		
		

		
		//sbQuery.append(") WHERE ROWNUM >=").append(startIndex).append(" AND ROWNUM < ")
		//		.append(endIndex);

		// System.out.println(sbQuery.toString());
		return sbQuery.toString();
	}

	public void closeResultset(ResultSet rs) {
		try {
			if (null != rs && !rs.isClosed()) {
				rs.close();
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	public void closeStatement(Statement stmt) {
		try {
			if (null != stmt && !stmt.isClosed()) {
				stmt.close();
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}	
	
	private String fetchResultAndTransform(String query, PreparedStatement stmt, String sliceID, Index index, ResultSet rs)
			throws SQLException, JsonGenerationException, JsonMappingException, IOException {
		if (logger.isDebugEnabled()) {
			// System.out.println("Query::");
			// System.out.println(query);
			logger.debug("Query:: " + query);
		}

		logger.info("Executing query: " + query);

		long startTime = System.currentTimeMillis();
		rs = stmt.executeQuery();
		long endTime = System.currentTimeMillis();
		String msg = new StringBuffer("Query Execution [").append(index.getDocumentType())
				.append("]: Time taken (seconds): ").append(((endTime - startTime) / 1000)).toString();
		System.out.println(msg);
		logger.info(msg);

		startTime = endTime;
		String resultsetJson = converter.convert(rs, index);
		endTime = System.currentTimeMillis();
		msg = new StringBuffer("Query Result Transformation to JSON [").append(index.getDocumentType())
				.append("]: Time taken (seconds): ").append(((endTime - startTime) / 1000)).toString();
		System.out.println(msg);
		logger.info(msg);

		return resultsetJson;
	}

	public String runQueryToJSONInBatches(Connection sliceDBConn, String query, String sliceID, String modifiedDateCol,
			int currentStart, int currentEnd, IndexOperationType opType, long lLastIndexRunTime, Index index)
			throws SQLException {
		String resultsetJson = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
//			int count = StringUtils.countMatches(query, "?");
//			query += " AND " + modifiedDateCol + " >= ?";

			query = returnBatchQuery(query, currentStart, currentEnd, modifiedDateCol);
			query += " AND " + modifiedDateCol + " >= ?";

			logger.info("Executing query: " + query);

			stmt = sliceDBConn.prepareStatement(query);
//			int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);

//			stmt.setLong(++sliceCounter, lLastIndexRunTime);
			stmt.setLong(1, lLastIndexRunTime);

			resultsetJson = fetchResultAndTransform(query, stmt, sliceID, index, rs);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			closeStatement(stmt);
			closeResultset(rs);
			// closeDBConnection(sliceDBConn);
		}

		return resultsetJson;
	}	
	
	public String runQueryToJSON(Connection sliceDBConn, Index index, long lLastIndexRunTime, String sliceID)
			throws SQLException, JsonGenerationException, JsonMappingException, IOException {
		String query = index.getQuery();
		//int count = StringUtils.countMatches(query, "?");

		query += " AND " + index.getModifiedDateKey() + " >= ?";
		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
		//int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);

		//stmt.setLong(++sliceCounter, lLastIndexRunTime);
		logger.info("Executing query: " + query);

		ResultSet rs = stmt.executeQuery();

		return converter.convert(rs, index);
	}
	
	public String runQueryToJSON(Connection sliceDBConn, Index index, ArrayList<Long> selectedIDsToIndex, String sliceID)
			throws SQLException, JsonGenerationException, JsonMappingException, IOException {
		String query = index.getQuery();
		StringBuffer sbQuery = new StringBuffer(query);

		sbQuery.append(" AND ").append(index.getIDKey()).append(" IN (");
		for (int i = 0; i < selectedIDsToIndex.size(); i++) {
			if (i == 0 && i != selectedIDsToIndex.size() - 1)
				sbQuery.append("?, ");
			else if (i < selectedIDsToIndex.size() - 1)
				sbQuery.append("?, ");
			else
				sbQuery.append("?");
		}
		sbQuery.append(")");
		System.out.println("Query: " + sbQuery.toString());
		if (logger.isDebugEnabled()) {
			logger.debug("Query: " + sbQuery.toString());
		}
		PreparedStatement stmt = sliceDBConn.prepareStatement(sbQuery.toString());
		int i = 1;
		for (Long rowID : selectedIDsToIndex) {
			stmt.setLong(i++, rowID);
		}
		logger.info("Executing query: " + query);

		ResultSet rs = stmt.executeQuery();

		return converter.convert(rs, index);
	}

	
	/**
	 * This should be used only for connection pool initialization
	 * @param bForConnectionPoolInitialization
	 * @return
	 */
	public HashMap<String, TenantDBConnectionData> getTenantDBsDetails(boolean bForConnectionPoolInitialization) {
		HashMap<String, TenantDBConnectionData> tenantDBMap = new HashMap<String, TenantDBConnectionData>();
		for (Tenant tenant : tenantsConfig.getAllTenants()) {
			TenantDBConnectionData data = TenantDBConnectionData.copyFrom(tenant, connectionReader.getConnectionData());
			tenantDBMap.put(tenant.getTenantId(), data);
			if(logger.isDebugEnabled()) {
				logger.debug("Adding to slice db map: " + data.toString());
			}
		}
		
		return tenantDBMap;
	}
	
	public HashMap<String, List<SelectiveIndexData>> getSelectiveIndexDataRecords(TenantDBConnectionData sliceDBConnData, String sliceID, boolean bDelRecordsPostIndexing) {
		HashMap<String, List<SelectiveIndexData>> mapRecords = new HashMap<String, List<SelectiveIndexData>>();
		//ArrayList<Long> recordsToDelete = new ArrayList<Long>();
		long lastTime = 0L;
		
		PreparedStatement stmt = null;
		ResultSet rsIndex = null;
		Connection sliceDBConn = getTenantDBConnection(sliceDBConnData);
	
		try {
			String fetchIndexStatusSql = "SELECT OBJECT_ID, OBJECT_CODE, OPERATION_TYPE, EVENT_TIME FROM ES_REALTIME_METADATA "
					+ " WHERE TENANT_ID=? ORDER BY OBJECT_ID DESC, OBJECT_CODE, EVENT_TIME";
			stmt = sliceDBConn.prepareStatement(fetchIndexStatusSql);
			stmt.setString(1, sliceID);
			logger.info("Executing query: " + fetchIndexStatusSql);
			
			rsIndex = stmt.executeQuery();
			HashMap<Long, Boolean> mapItems = new HashMap<Long, Boolean>();
			while (rsIndex.next()) {
				logger.info("Getting record from resultset...");
				
				long eventTime = rsIndex.getLong("EVENT_TIME");
				if(lastTime < eventTime) {
					lastTime = eventTime;
				}
				
				//Continue if duplicate item found
				//Basically if the same item has been modified couple of times - consider the final operation
				long itemID = rsIndex.getLong("OBJECT_ID");
				logger.info("Got object id for indexing: " + itemID);
				System.out.println("Got object id for indexing: " + itemID);
				
				Boolean bExists = mapItems.get(itemID);
				if(null == bExists || !bExists.booleanValue()) {
					logger.info("Added item <" + itemID + "> for indexing.");
					System.out.println("Added item <" + itemID + "> for indexing.");
					logger.info("Added item <" + itemID + "> for indexing."); 
					mapItems.put(itemID, true);
				} else {
					logger.info("Item <" + itemID + "> already exists for indexing, skipping...");
					System.out.println("Item <" + itemID + "> already exists for indexing, skipping...");
					continue;
				}
				
				String indexDocType = rsIndex.getString("OBJECT_CODE");
				String operationTypeStr = rsIndex.getString("OPERATION_TYPE");
				
				SelectiveIndexData data = new SelectiveIndexData(sliceID, itemID, indexDocType, operationTypeStr);
				
				List<SelectiveIndexData> records = mapRecords.get(indexDocType);
				if(null == records) {
					records = new ArrayList<SelectiveIndexData>();
					mapRecords.put(indexDocType, records);
				}
				
				logger.info("Added following data for near realtime indexing: " + data);
				System.out.println("Added following data for near realtime indexing: " + data);
				records.add(data);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			System.out.println("Exception occurred while adding record for indexing:: " + ex.getMessage());
		} finally {
			closeStatement(stmt);
			closeResultset(rsIndex);
		}
		
		if(bDelRecordsPostIndexing) {
			//delete the records - as those are picked up for indexing already
			PreparedStatement deleteStmt = null;
			try {
				if(lastTime > 0) {
					StringBuffer sbQuery = new StringBuffer("DELETE FROM ES_REALTIME_METADATA WHERE EVENT_TIME <= ? ");
					deleteStmt = sliceDBConn.prepareStatement(sbQuery.toString());
					deleteStmt.setLong(1, lastTime);
					int total = deleteStmt.executeUpdate();
					System.out.println("Near realtime indexer deleted <" + total +"> records.");
				}
			} catch (Exception ex) {
				logger.error(ex.getMessage());
			} finally {
				closeStatement(deleteStmt);
				closeDBConnection(sliceDBConn, sliceDBConnData.getDbType());
			}
		}
		
		return mapRecords;
	}	
	
////	private Connection getPlatformDBConnection() {
////		ConnectionData platformDBConnData = connectionReader.getConnectionData();
////		String platformDBPassword = platformDBConnData.getPassword();
////		Connection platformDBConn = dbConnect(platformDBConnData.getUrl(), platformDBConnData.getUsername(), platformDBPassword,
////				platformDBConnData.getDriverClassName());
////		return platformDBConn;
////	}
//	
//	private Connection getPlatformDBConnection() {
//		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();		
//		DBConnectionPool connPool = connFactory.getAndInitializeDBConnectionPool(DBType.PlatformDBPrimary);
//		Connection platformDBConn = null;
//		try {
//			platformDBConn = connPool.getActualPool().borrowObject();//.getDataSource().getConnection();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		//System.out.println("pP: " + (++pP));
//		return platformDBConn;
//	}	
//	
//	/**
//	 * Read-only calls to platform DB should be made by getting secondary platform DB connection
//	 * @return
//	 */
//	public Connection getPlatformSecondaryDBConnection() {
//		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();		
//		DBConnectionPool connPool = connFactory.getAndInitializeDBConnectionPool(DBType.PlatformDBSecondary);
//		Connection platformDBConn = null;
//		try {
//			platformDBConn = connPool.getActualPool().borrowObject();//.getDataSource().getConnection();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		//System.out.println("pS: " + (++pS));
//		return platformDBConn;
//	}
//	
//	public Connection getSlicePrimaryDBConnection(SliceDBConnectionData connData) {
//		//System.out.println("sP: " + (++sP));
//
//		return getSliceDBConnection(connData, DBType.SliceDBPrimary);
//	}
//
//	public Connection getSliceSecondaryDBConnection(SliceDBConnectionData connData) {
//		//System.out.println("sS: " + (++sS));
//		
//		return getSliceDBConnection(connData, DBType.SliceDBSecondary);
//	}
//	
//	private Connection getTenantDBConnection(TenantDBConnectionData connData, DBType tenantDBType) {
//		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();		
//		connFactory.setTenantDBConnData(connData);
//		
//		DBConnectionPool connPool = connFactory.getAndInitializeDBConnectionPool(tenantDBType);
//		Connection sliceDBConn = null;
//		try {
//			sliceDBConn = connPool.getActualPool().borrowObject();//  getDataSource().getConnection();
//			sliceDBConn.setCatalog(connData.getDbSchemaName());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		return sliceDBConn;
//	}
//	
////	private Connection dbConnect(String dbConnectString, String dbUserId,
////			String dbPassword, String driverClassName) {
////		if(logger.isDebugEnabled()) {
////			logger.debug("Connecting to : " + dbConnectString);
////		}
////		
////		try {
////			Class.forName(driverClassName);
////			Connection conn = DriverManager.getConnection(dbConnectString,
////					dbUserId, dbPassword);
////			return conn;
////		} catch (Exception ex) {
////			logger.error(ex.getMessage());
////			ex.printStackTrace();
////		}
////		
////		if(logger.isDebugEnabled()) {
////			logger.debug("Returning NULL connection object, this will cause problem.");
////			logger.debug("Please make sure you have provided proper platform database configuration in db-config.xml file. If the passwords are encrypted, provide correct configuration in db-config.xml file.");
////			logger.debug("Please ensure platform database has correct login information for all slice databases in IU_DATABASES table.");
////		}
////		
////		return null;
////	}
//	
////	public Connection getSliceDBConnection(SliceDBConnectionData connData) {
////		StringBuilder sbConnString = new StringBuilder("jdbc:jtds:sqlserver://");
////		sbConnString.append(connData.getDbServer()).append(":").append(connData.getDbPort()).append("/").append(connData.getDbSchemaName()).append(";");
////		return dbConnect(sbConnString.toString(), connData.getDbUser(), connData.getDbPassword(), connData.getDbDriver());
////	}
//	
//	/**
//	 * 
//	 * @param sliceID - for which slice
//	 * @param indexDocType - index document type (like - Ticket, KBArticle, Contact, etc.)
//	 * @throws SQLException 
//	 */
//	public void updateIndexCompletionStatus(int sliceID, String indexDocType, IndexStatus status) {
//		Connection platformDBConnection = null;
//		PreparedStatement stmt = null;
//		try {
//			platformDBConnection = getPlatformDBConnection();
//			platformDBConnection.setAutoCommit(true);
//			long currentDateTime = System.currentTimeMillis();
//			currentDateTime = currentDateTime/1000;
//			String updateIndexSql = null;
//			if(isIndexStatusExists(platformDBConnection, sliceID, indexDocType)) {
//				updateIndexSql = "UPDATE ES_INDEX_STATUS SET LAST_INDEXED_ON = ?, INDEX_STATUS = ? WHERE SLICE = ? AND INDEX_DOCUMENT_TYPE = ?";
//			} else {
//				updateIndexSql = "INSERT INTO ES_INDEX_STATUS (LAST_INDEXED_ON, INDEX_STATUS, SLICE, INDEX_DOCUMENT_TYPE) VALUES (?, ?, ?, ?, ?)";
//			}
//			stmt = platformDBConnection.prepareStatement(updateIndexSql);
//			stmt.setLong(1, currentDateTime);
//			stmt.setString(2, status.getValue());
//			stmt.setInt(3, sliceID);
//			stmt.setString(4, indexDocType);
//			
//			stmt.executeUpdate();
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			closeDBConnection(platformDBConnection, DBType.PlatformDBPrimary);
//		}		
//	}
//	
//	
//	/**
//	 * When it picks up an index for creation, some of the status is updated
//	 * @param sliceID - for which slice
//	 * @param indexDocType - index document type (like - Ticket, KBArticle, Contact, etc.)
//	 * @throws SQLException 
//	 */
//	public void updateIndexInitiatedStatus(int sliceID, String indexDocType, IndexOperationType opType) {
//		Connection platformDBConnection = null;
//		PreparedStatement stmt = null;
//		try {
//			platformDBConnection = getPlatformDBConnection();
//			platformDBConnection.setAutoCommit(true);
//			long currentDateTime = System.currentTimeMillis();
//			currentDateTime = currentDateTime/1000;
//			String updateIndexSql = null;
//			if(isIndexStatusExists(platformDBConnection, sliceID, indexDocType)) {
//				updateIndexSql = "UPDATE ES_INDEX_STATUS SET LAST_INDEXED_ON = ?, INDEX_STATUS = ?, LAST_INDEX_TYPE = ?, EXECUTION_STARTED_AT = ? WHERE SLICE = ? AND INDEX_DOCUMENT_TYPE = ?";
//			} else {
//				updateIndexSql = "INSERT INTO ES_INDEX_STATUS (LAST_INDEXED_ON, INDEX_STATUS, LAST_INDEX_TYPE, EXECUTION_STARTED_AT, SLICE, INDEX_DOCUMENT_TYPE) VALUES (?, ?, ?, ?, ?, ?)";
//			}
//			stmt = platformDBConnection.prepareStatement(updateIndexSql);
//			stmt.setLong(1, currentDateTime);
//			stmt.setString(2, IndexStatus.Running.getValue());
//			stmt.setString(3, opType.toString());
//			stmt.setLong(4, currentDateTime);
//			stmt.setInt(5, sliceID);
//			stmt.setString(6, indexDocType);
//			
//			stmt.executeUpdate();
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			closeDBConnection(platformDBConnection, DBType.PlatformDBPrimary);
//		}		
//	}
//	
//	private boolean isIndexStatusExists(Connection platformDBConnection, int sliceID, String indexDocType) {
//		PreparedStatement stmt = null;
//		ResultSet rsIndex = null;
//		boolean bRecordExists = false;
//		try {
//			String fetchIndexStatusSql = "SELECT 1 FROM ES_INDEX_STATUS WITH (NOLOCK) WHERE SLICE = ? AND INDEX_DOCUMENT_TYPE = ?";
//			stmt = platformDBConnection.prepareStatement(fetchIndexStatusSql);
//			stmt.setInt(1, sliceID);
//			stmt.setString(2, indexDocType);
//			
//			logger.info("Executing query: " + fetchIndexStatusSql);
//
//			rsIndex = stmt.executeQuery();
//			while (rsIndex.next()) {
//				bRecordExists = true;
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			if(null != rsIndex) {
//				try {
//					rsIndex.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		return bRecordExists;
//	}
//	
//	/**
//	 * Check if there is any record as RUNNING status for the current index, if exists - another indexing operation cannot run
//	 * @param platformDBConnection
//	 * @param sliceID
//	 * @param indexDocType
//	 * @return
//	 */
//	public boolean isAlreadyIndexing(Connection platformDBConnection, int sliceID, String indexDocType) {
//		PreparedStatement stmt = null;
//		ResultSet rsIndex = null;
//		boolean bRecordExistsForRunning = false;
//		try {
//			String fetchIndexStatusSql = "SELECT 1 FROM ES_INDEX_STATUS WHERE SLICE = ? AND INDEX_DOCUMENT_TYPE = ? AND INDEX_STATUS = ?";
//			stmt = platformDBConnection.prepareStatement(fetchIndexStatusSql);
//			stmt.setInt(1, sliceID);
//			stmt.setString(2, indexDocType);
//			stmt.setString(3, IndexStatus.Running.getValue());
//			
//			logger.info("Executing query: " + fetchIndexStatusSql);
//
//			rsIndex = stmt.executeQuery();
//			while (rsIndex.next()) {
//				bRecordExistsForRunning = true;
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			if(null != rsIndex) {
//				try {
//					rsIndex.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		return bRecordExistsForRunning;
//	}	
//	
//	public SliceIndexMetadata getSlicesIndexMetaData() {
//		SliceIndexMetadata indexMetadataForSlices = new SliceIndexMetadata();
//		HashMap<Integer, HashMap<String, SliceIndexMetadataRecord>> mapIndexDataForSlices = new HashMap<Integer, HashMap<String, SliceIndexMetadataRecord>>();
//		indexMetadataForSlices.setMapIndexDataForSlices(mapIndexDataForSlices);
//		Statement stmt = null;
//		ResultSet rsIndex = null;
//		Connection platformDBConnection = getPlatformSecondaryDBConnection();
//		try {
//			String fetchIndexStatusSql = "SELECT SLICE, INDEX_DOCUMENT_TYPE, LAST_INDEXED_ON, INDEX_STATUS, LAST_INDEX_TYPE, EXECUTION_STARTED_AT FROM ES_INDEX_STATUS ORDER BY SLICE";
//			stmt = platformDBConnection.createStatement();
//			logger.info("Executing query: " + fetchIndexStatusSql);
//
//			rsIndex = stmt.executeQuery(fetchIndexStatusSql);
//			while (rsIndex.next()) {
//				int sliceID = rsIndex.getInt(1);
//				String indexDocType = rsIndex.getString(2);
//				long lastIndxedOn = rsIndex.getLong(3);
//				String indexStatus = rsIndex.getString(4);
//				String lastIndexType = rsIndex.getString(5);
//				long lIndexStartedAt = rsIndex.getLong(6);
//				HashMap<String, SliceIndexMetadataRecord> mapIndexDataForSlice = mapIndexDataForSlices.get(sliceID);
//				if(null == mapIndexDataForSlice) {
//					mapIndexDataForSlice = new HashMap<String, SliceIndexMetadataRecord>();
//					mapIndexDataForSlices.put(sliceID, mapIndexDataForSlice);
//				}
//				SliceIndexMetadataRecord record = new SliceIndexMetadataRecord(sliceID, indexDocType, lastIndxedOn, indexStatus, lastIndexType, lIndexStartedAt);
//				mapIndexDataForSlice.put(indexDocType, record);
//				if(logger.isDebugEnabled()) {
//					logger.debug(record.toString());
//				}
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			if(null != rsIndex) {
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
//	
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
//				if(null == mapIndexDataForSlice) {
//					mapIndexDataForSlice = new HashMap<String, SliceIndexMetadataRecord>();
//					mapIndexDataForSlices.put(sliceID, mapIndexDataForSlice);
//				}
//				
//				SliceIndexMetadataRecord record = new SliceIndexMetadataRecord(sliceID, indexDocType, lastIndxedOn, indexStatus, lastIndexType, lIndexStartedAt);
//				mapIndexDataForSlice.put(indexDocType, record);
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			if(null != rsIndex) {
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
//
//	/**
//	 * Basically used for Full Indexing (Non-batch version)
//	 * @param sliceDBConn
//	 * @param query
//	 * @param sliceID
//	 * @return
//	 * @throws SQLException
//	 * @throws IOException 
//	 * @throws JsonMappingException 
//	 * @throws JsonGenerationException 
//	 */
//	public String runQueryToJSON (Connection sliceDBConn, String query, int sliceID, Index index) throws SQLException, JsonGenerationException, JsonMappingException, IOException {
//		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
//		int count = StringUtils.countMatches(query, "?");
//
//		updateSliceCounter(query, stmt, sliceID, count);
//		logger.info("Executing query: " + query);
//
//		ResultSet rs = stmt.executeQuery();
//		
//		return converter.convert(rs, index);
//	}
//	
//	/**
//	 * Basically used for Full Indexing (Batch version)
//	 * @param sliceDBConn
//	 * @param query
//	 * @param sliceID
//	 * @param opType 
//	 * @param currentEnd 
//	 * @param currentStart 
//	 * @return
//	 * @throws SQLException
//	 */
//	public String runQueryToJSONInBatches (Connection sliceDBConn, String query, int sliceID, String modifiedDateCol, 
//			int currentStart, int currentEnd, Index index) throws SQLException {
//		String resultsetJson = null;
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		try {
//			query = returnBatchQuery(query, currentStart, currentEnd, modifiedDateCol);
//			
//			stmt = sliceDBConn.prepareStatement(query);
//			int sliceCounter = StringUtils.countMatches(query, "?");
//			updateSliceCounter(query, stmt, sliceID, sliceCounter);
//			logger.info("Executing query: " + query);
//
//			resultsetJson = fetchResultAndTransform(query, stmt, sliceID, index, rs);		
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			closeStatement(stmt);
//			closeResultset(rs);
//			//closeDBConnection(sliceDBConn);
//		}
//		
//		return resultsetJson;
//	}
//	
//	public String runQueryToJSONInBatches (Connection sliceDBConn, String query, int sliceID, String modifiedDateCol, 
//			int currentStart, int currentEnd, IndexOperationType opType, long lLastIndexRunTime, Index index) throws SQLException {
//		String resultsetJson = null;
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		try {
//			int count = StringUtils.countMatches(query, "?");
//			query += " AND "+ modifiedDateCol +" >= ?";
//
//			query = returnBatchQuery(query, currentStart, currentEnd, modifiedDateCol);
//			logger.info("Executing query: " + query);
//
//			stmt = sliceDBConn.prepareStatement(query);
//			int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);
//			
//			stmt.setLong(++sliceCounter, lLastIndexRunTime);
//	
//			resultsetJson = fetchResultAndTransform(query, stmt, sliceID, index, rs);		
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			closeStatement(stmt);
//			closeResultset(rs);
//			//closeDBConnection(sliceDBConn);
//		}
//		
//		return resultsetJson;
//	}	
//	
//	/**
//	 * Basically used for Incremental Indexing (Batch version)
//	 * @param sliceDBConn
//	 * @param query
//	 * @param sliceID
//	 * @return
//	 * @throws SQLException
//	 */	
////	public String runQueryToJSONInBatches(Connection sliceDBConn, Index index,
////			long lLastIndexRunTime, int sliceID, String modifiedDateCol) throws SQLException {
////		String query = index.getQuery();
////		query = returnBatchQuery(query, 1, indexConfig.getIndexConfiguration().getBatchCount(), modifiedDateCol);
////		
////		int count = StringUtils.countMatches(query, "?");
////
////		query += " AND "+ index.getModifiedDateKey() +" > ?";
////		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
////		int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);
////		
////		stmt.setLong(++sliceCounter, lLastIndexRunTime);
////		ResultSet rs = stmt.executeQuery();
////		
////		return converter.convert(rs);
////	}	
//
////	private String returnBatchQuery(String query, int startIndex, int endIndex, String modifiedDateCol) {
////		String queryTemp = query.substring(query.toLowerCase().indexOf("select") + 6);
////		
////		StringBuffer sbQuery = new StringBuffer("SELECT * FROM (SELECT ROW_NUMBER() OVER ( ORDER BY "+ modifiedDateCol +" ) AS RowNum, "); 
////		sbQuery.append(queryTemp);
////		sbQuery.append(") AS RowConstrainedResult WHERE   RowNum >=" ).append(startIndex).append(" AND RowNum < ").append(endIndex).append(" ORDER BY RowNum");
////		
////		//System.out.println(sbQuery.toString());
////		return sbQuery.toString();
////	}
//	
//	/**
//	 * Basically used for Full Indexing (Batch version)
//	 * @param sliceDBConn
//	 * @param query
//	 * @param sliceID
//	 * @param opType 
//	 * @param currentEnd 
//	 * @param currentStart 
//	 * @return
//	 * @throws SQLException
//	 * @throws IOException 
//	 * @throws JsonMappingException 
//	 * @throws JsonGenerationException 
//	 */
////	public String runQueryToJSONInBatches (Connection sliceDBConn, String query, int sliceID, String modifiedDateCol, 
////			int currentStart, int currentEnd, IndexOperationType opType) throws SQLException {
////		
////		query = returnBatchQuery(query, currentStart, currentEnd, modifiedDateCol);
////		
////		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
////		int count = StringUtils.countMatches(query, "?");
////
////		updateSliceCounter(query, stmt, sliceID, count);
////		ResultSet rs = stmt.executeQuery();
////		
////		return converter.convert(rs);
////	}
//	
//	private String fetchResultAndTransform(String query, PreparedStatement stmt, int sliceID, Index index, ResultSet rs) throws SQLException, JsonGenerationException, JsonMappingException, IOException {
////		if(logger.isDebugEnabled()) {
////			//System.out.println("Query::");
////			//System.out.println(query);
////			logger.debug("Query:: "+ query);
////		}
//		
//		logger.info("Executing query: " + query);
//		
//		long startTime = System.currentTimeMillis();
//		rs = stmt.executeQuery();
//		long endTime = System.currentTimeMillis();
//		String msg = new StringBuffer("Query Execution [").append(index.getDocumentType()).append("]: Time taken (seconds): ").append(((endTime-startTime)/1000)).toString();
//		System.out.println(msg);
//		logger.info(msg);
//		
//		startTime = endTime;
//		String resultsetJson = converter.convert(rs, index);
//		endTime = System.currentTimeMillis();
//		msg = new StringBuffer("Query Result Transformation to JSON [").append(index.getDocumentType()).append("]: Time taken (seconds): ").append(((endTime-startTime)/1000)).toString();
//		System.out.println(msg);
//		logger.info(msg);	
//		
//		return resultsetJson;
//	}
//
//
//	/**
//	 * Basically used for Incremental Indexing (Batch version)
//	 * @param sliceDBConn
//	 * @param query
//	 * @param sliceID
//	 * @return
//	 * @throws SQLException
//	 */	
////	public String runQueryToJSONInBatches(Connection sliceDBConn, Index index,
////			long lLastIndexRunTime, int sliceID, String modifiedDateCol) throws SQLException {
////		String query = index.getQuery();
////		query = returnBatchQuery(query, 1, indexConfig.getIndexConfiguration().getBatchCount(), modifiedDateCol);
////		
////		int count = StringUtils.countMatches(query, "?");
////
////		query += " AND "+ index.getModifiedDateKey() +" > ?";
////		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
////		int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);
////		
////		stmt.setLong(++sliceCounter, lLastIndexRunTime);
////		ResultSet rs = stmt.executeQuery();
////		
////		return converter.convert(rs);
////	}	
//
//	private String returnBatchQuery(String query, int startIndex, int endIndex, String modifiedDateCol) {
//		String queryTemp = query.substring(query.toLowerCase().indexOf("select") + 6);
//		
//		StringBuffer sbQuery = new StringBuffer("SELECT * FROM (SELECT ROW_NUMBER() OVER ( ORDER BY "+ modifiedDateCol +" ) AS RowNum, "); 
//		sbQuery.append(queryTemp);
//		sbQuery.append(") AS RowConstrainedResult WHERE   RowNum >=" ).append(startIndex).append(" AND RowNum < ").append(endIndex).append(" ORDER BY RowNum");
//		
//		//System.out.println(sbQuery.toString());
//		return sbQuery.toString();
//	}
//
//	public String runQueryToJSON(Connection sliceDBConn, Index index,
//			long lLastIndexRunTime, int sliceID) throws SQLException, JsonGenerationException, JsonMappingException, IOException {
//		String query = index.getQuery();
//		int count = StringUtils.countMatches(query, "?");
//
//		query += " AND "+ index.getModifiedDateKey() +" >= ?";
//		PreparedStatement stmt = sliceDBConn.prepareStatement(query);
//		int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);
//		
//		stmt.setLong(++sliceCounter, lLastIndexRunTime);
//		logger.info("Executing query: " + query);
//		
//		ResultSet rs = stmt.executeQuery();
//		
//		return converter.convert(rs, index);
//	}
//
//	public String runQueryToJSON(Connection sliceDBConn, Index index,
//			ArrayList<Long> selectedIDsToIndex, int sliceID) throws SQLException, JsonGenerationException, JsonMappingException, IOException {
//		String query = index.getQuery();
//		StringBuffer sbQuery = new StringBuffer(query);
//		int count = StringUtils.countMatches(query, "?");
//		
//		sbQuery.append(" AND ").append(index.getIDKey()).append(" IN (" );
//		for (int i=0; i<selectedIDsToIndex.size(); i++) {
//			if (i == 0 && i != selectedIDsToIndex.size()-1)
//				sbQuery.append("?, ");
//			else if(i < selectedIDsToIndex.size()-1)
//				sbQuery.append("?, ");
//			else 
//				sbQuery.append("?");
//		}
//		sbQuery.append(")");
//		System.out.println("Query: " + sbQuery.toString());
//		if(logger.isDebugEnabled()) {
//			logger.debug("Query: " + sbQuery.toString());
//		}
//		PreparedStatement stmt = sliceDBConn.prepareStatement(sbQuery.toString());
//		int sliceCounter = updateSliceCounter(query, stmt, sliceID, count);
//		int i = sliceCounter+1;
//		for (Long rowID : selectedIDsToIndex) {
//			stmt.setLong(i++, rowID);
//		}
//		logger.info("Executing query: " + query);
//		
//		ResultSet rs = stmt.executeQuery();
//		
//		return converter.convert(rs, index);
//	}	
//	
//	private int updateSliceCounter(String query, PreparedStatement stmt, int sliceID, int count) throws SQLException {
//		for(int i=1; i<=count; i++) {
//			stmt.setInt(i, sliceID);
//		}
//		return count;
//	}
//	
//	public boolean isSliceConfigForElasticSearchEnabled(SliceDBConnectionData sliceConnData) {
//		boolean bSliceConfigEnabled = false;
//		Connection sliceConn = null;
//	
//		try {
//			sliceConn = getSliceSecondaryDBConnection(sliceConnData);
//			bSliceConfigEnabled = isSliceConfigESSearchEnabledForSlice(sliceConn, sliceConnData.getSlice(), true);
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error(ex.getMessage());
//		} finally {
//			returnConnection(sliceConn, DBType.SliceDBSecondary);
//		}
//		
//		return bSliceConfigEnabled;
//	}
//
//	public void closeDBConnection(Connection dbConn, DBType dbType) {
//		returnConnection(dbConn, dbType);
//	}
//	
//	public void closeResultset(ResultSet rs) {
//		try {
//			if(null != rs && !rs.isClosed()) {
//				rs.close();
//			}
//		} catch (SQLException e) {
//			logger.error(e.getMessage());
//			e.printStackTrace();
//		}			
//	}
//	
//	public void closeStatement(Statement stmt) {
//		try {
//			if(null != stmt && !stmt.isClosed()) {
//				stmt.close();
//			}
//		} catch (SQLException e) {
//			logger.error(e.getMessage());
//			e.printStackTrace();
//		}	
//	}	
//
//	public boolean isSliceConfigESSearchEnabledForSlice(Connection sliceConn, int sliceID, boolean bCloseConnection) {
//		PreparedStatement stmt = null;
//		ResultSet rsIndex = null;
//		try {
//			String fetchSliceConfigStatusSql = "SELECT ATTRIB_VALUE FROM IU_SLICE_CONFIGURATION WITH (NOLOCK)"
//					+ " where ATTRIB_NAME='ENABLE_ELASTIC_SEARCH' and SLICE = ? ";
//			stmt = sliceConn.prepareStatement(fetchSliceConfigStatusSql);
//			stmt.setInt(1, sliceID);
//			logger.info("Executing query: " + fetchSliceConfigStatusSql);
//			
//			rsIndex = stmt.executeQuery();
//			while (rsIndex.next()) {
//				String sliceConfigEnabled = rsIndex.getString(1);
//				return Boolean.parseBoolean(sliceConfigEnabled);
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			if(null != stmt) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			if(null != rsIndex) {
//				try {
//					rsIndex.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			
//			if(bCloseConnection && null != sliceConn) {
//				returnConnection(sliceConn, DBType.SliceDBSecondary);
//			}
//		}
//		
//		return false;
//	}
//
//	public HashMap<String, List<SelectiveIndexData>> getSelectiveIndexDataRecords(SliceDBConnectionData sliceDBConnData, int sliceID, boolean bDelRecordsPostIndexing) {
//		HashMap<String, List<SelectiveIndexData>> mapRecords = new HashMap<String, List<SelectiveIndexData>>();
//		ArrayList<Long> recordsToDelete = new ArrayList<Long>();
//		
//		PreparedStatement stmt = null;
//		ResultSet rsIndex = null;
//		Connection sliceDBConn = getSliceSecondaryDBConnection(sliceDBConnData);
//
//		try {
//			String fetchIndexStatusSql = "SELECT ROW_ID, ITEM_ID, DOCUMENT_TYPE, OPERATION_TYPE FROM ES_INDEX_RECORDS WITH (NOLOCK) "
//					+ " WHERE SLICE=? ORDER BY ROW_ID DESC, DOCUMENT_TYPE";
//			stmt = sliceDBConn.prepareStatement(fetchIndexStatusSql);
//			stmt.setInt(1, sliceID);
//			logger.info("Executing query: " + fetchIndexStatusSql);
//			
//			rsIndex = stmt.executeQuery();
//			HashMap<Long, Boolean> mapItems = new HashMap<Long, Boolean>();
//			while (rsIndex.next()) {
//				logger.info("Getting record from resultset...");
//				
//				//Continue if duplicate item found
//				//Basically if the same item has been modified couple of times - consider the final operation
//				long itemID = rsIndex.getLong("ITEM_ID");
//				logger.info("Got item id for indexing: " + itemID);
//				System.out.println("Got item id for indexing: " + itemID);
//
//				long rowID = rsIndex.getLong("ROW_ID");
//				logger.info("Got row id for indexing: " + rowID);
//				System.out.println("Got row id for indexing: " + rowID);
//				
//				recordsToDelete.add(rowID);
//				
//				Boolean bExists = mapItems.get(itemID);
//				if(null == bExists || !bExists.booleanValue()) {
//					logger.info("Added item <" + itemID + "> for indexing.");
//					System.out.println("Added item <" + itemID + "> for indexing.");
//					logger.info("Added item <" + itemID + "> for indexing."); 
//					mapItems.put(itemID, true);
//				} else {
//					logger.info("Item <" + itemID + "> already exists for indexing, skipping...");
//					System.out.println("Item <" + itemID + "> already exists for indexing, skipping...");
//					continue;
//				}
//				
//				String indexDocType = rsIndex.getString("DOCUMENT_TYPE");
//				String operationTypeStr = rsIndex.getString("OPERATION_TYPE");
//				
//				SelectiveIndexData data = new SelectiveIndexData(rowID, sliceID, itemID, indexDocType, operationTypeStr);
//				
//				List<SelectiveIndexData> records = mapRecords.get(indexDocType);
//				if(null == records) {
//					records = new ArrayList<SelectiveIndexData>();
//					mapRecords.put(indexDocType, records);
//				}
//				
//				logger.info("Added following data for near realtime indexing: " + data);
//				System.out.println("Added following data for near realtime indexing: " + data);
//				records.add(data);
//			}
//		} catch (Exception ex) {
//			logger.error(ex.getMessage());
//			System.out.println("Exception occurred while adding record for indexing:: " + ex.getMessage());
//		} finally {
//			closeStatement(stmt);
//			closeResultset(rsIndex);
//			closeDBConnection(sliceDBConn, DBType.SliceDBSecondary);
//		}
//		
//		if(bDelRecordsPostIndexing) {
//			//delete the records - as those are picked up for indexing already
//			Connection sliceDBConnUpdate = getSlicePrimaryDBConnection(sliceDBConnData);
//			PreparedStatement deleteStmt = null;
//			try {
//				if(recordsToDelete.size() > 0) {
//					StringBuffer sbQuery = new StringBuffer("DELETE FROM ES_INDEX_RECORDS WHERE ROW_ID IN (");
//	
//					for (int i=0; i<recordsToDelete.size(); i++) {
//						if (i == 0 && i != recordsToDelete.size()-1)
//							sbQuery.append("?, ");
//						else if(i < recordsToDelete.size()-1)
//							sbQuery.append("?, ");
//						else 
//							sbQuery.append("?");
//					}
//					sbQuery.append(")");
//					deleteStmt = sliceDBConnUpdate.prepareStatement(sbQuery.toString());
//					for (int i=0; i<recordsToDelete.size(); i++) {
//						deleteStmt.setLong(i+1, recordsToDelete.get(i));
//					}
//					
//					int total = deleteStmt.executeUpdate();
//					System.out.println("Near realtime indexer deleted <" + total +"> records.");
//				}
//			} catch (Exception ex) {
//				logger.error(ex.getMessage());
//			} finally {
//				closeStatement(deleteStmt);
//				closeDBConnection(sliceDBConnUpdate, DBType.SliceDBPrimary);
//			}
//		}
//		
//		return mapRecords;
//	}
//	
//	public void returnConnection(Connection conn, DBType dbType) {
//		DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();		
//		connFactory.returnDBConnection(conn, dbType);
//		
////		System.out.println("============ RELEASE::START ==========");
////		switch(dbType){
////		case PlatformDBPrimary:
////			System.out.println("pP: " + (--pP));
////			break;
////			
////		case PlatformDBSecondary:
////			System.out.println("pS: " + (--pS));
////			break;
////			
////		case SliceDBPrimary:
////			System.out.println("sP: " + (--sP));
////			break;
////			
////		case SliceDBSecondary:
////			System.out.println("sS: " + (--sS));
////			break;
////		}
////		System.out.println("============= RELEASE::END ===========");
//		
//	}
//
//
	public void deleteIndexStatusRecord(String sliceID, String typeName, long startedAt) {
//		Connection platformDBConn = getPlatformDBConnection();
//		try {
//			long currentDateTime = System.currentTimeMillis();
//			currentDateTime = currentDateTime/1000;			
//			PreparedStatement pstmt = platformDBConn.prepareStatement("UPDATE ES_INDEX_STATUS SET INDEX_STATUS=?, EXECUTION_STARTED_AT=?, LAST_INDEXED_ON=? WHERE SLICE=? AND INDEX_DOCUMENT_TYPE=?");
//			pstmt.setString(1, "Deleted");
//			pstmt.setLong(2, startedAt);
//			pstmt.setLong(3, currentDateTime);
//			pstmt.setInt(4, sliceID);
//			pstmt.setString(5, typeName);
//			
//			pstmt.executeUpdate();
//		} catch (SQLException e) {
//			e.printStackTrace();
//			logger.error(e.getMessage());
//		} finally {
//			returnConnection(platformDBConn, DBType.PlatformDBPrimary);
//		}
	}
}
