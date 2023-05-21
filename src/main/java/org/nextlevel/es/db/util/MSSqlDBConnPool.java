package org.nextlevel.es.db.util;

import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * 
 * @author nextlevel
 *
 */
public class MSSqlDBConnPool extends DBConnectionPool {
	private final String SLICE_DB_URL = "jdbc:sqlserver://{0}:{1};databaseName={2}"; //;MultiSubnetFailover=True
	
	private static MSSqlDBConnPool instance = new MSSqlDBConnPool();

	private MSSqlDBConnPool(){
		super();
		this.dbType = DBType.MSSQL;
	}
	public static MSSqlDBConnPool getInstance() {
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
		String defaultSliceDB = connData.getDefaultTenantDB(DBType.MSSQL).getDefaultTenantDBName();
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
