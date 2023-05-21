package org.nextlevel.es.db.util;

import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * 
 * @author sk
 *
 */
public class MySQLDBConnPool extends DBConnectionPool {
	private final String SLICE_DB_URL = "jdbc:mysql://{0}:{1}/{2}"; //jdbc:mysql://localhost:3306/skizaa_db_v1
	
	private static MySQLDBConnPool instance = new MySQLDBConnPool();

	private MySQLDBConnPool(){
		super();
		this.dbType = DBType.MYSQL;
	}
	public static MySQLDBConnPool getInstance() {
		return instance;
	}
	
	@Override
	protected void initializePool(DBType sliceDBType) {
		if(null != getTenantDBConnData()) {
			dataSource = setupDataSource(sliceDBType);
		}		
	}
	
	@Override
	protected String getConnectionURL() {
		//String defaultSliceDB = connData.getDefaultTenantDB(DBType.ORACLE).getDefaultTenantDBName();
		String defaultSliceDB = getTenantDBConnData().getDbSchemaName();
		if(null != getTenantDBConnData()) {
			databaseServerName = getTenantDBConnData().getDbServer();
			databaseServerPort = String.valueOf(getTenantDBConnData().getDbPort());
		}
		
		String URL = getFormattedConnectionURL(SLICE_DB_URL, databaseServerName, databaseServerPort, defaultSliceDB);
		return URL;
	}
	
	@Override
	protected String getDBPassword() {
		return (null != getTenantDBConnData())?getTenantDBConnData().getDbPassword():null;
	}
	@Override
	protected String getDBUser() {
		return (null != getTenantDBConnData())?getTenantDBConnData().getDbUser():null;
	}
	
	@Override
	public TenantDBConnectionData getTenantDBConnData() {
		return tenantDBConnData;
	}
	@Override
	public void setTenantDBConnData(TenantDBConnectionData tenantDBConnData) {
		this.tenantDBConnData = tenantDBConnData;
	}	
	
	@Override
	protected GenericObjectPool<PoolableConnection> getActualPool() {
		GenericObjectPool<PoolableConnection> connectionPool = mapConnectionPool.get(getTenantDBConnData().getDbServer()); 
		return connectionPool;
	}

}
