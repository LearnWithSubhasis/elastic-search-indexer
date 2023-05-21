package org.nextlevel.es.db.util;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

import org.nextlevel.es.conn.ConnectionData;
import org.nextlevel.es.conn.ConnectionReader;
import org.nextlevel.es.conn.DefaultDatabase;

/**
 * The factory class for DB connection pool. Based on the database type it returns a specific pool instance.
 * @author nextlevel
 *
 */
public class DBConnectionPoolFactory {
	private static DBConnectionPoolFactory instance = new DBConnectionPoolFactory();
	private ConnectionData connectionConfigData = ConnectionReader.getInstance().getConnectionData();
	private TenantDBConnectionData tenantDBConnData;
	
	private DBConnectionPoolFactory(){
		initializeSQLDriver();
	}
	
	public static DBConnectionPoolFactory getInstance() {
		return instance;
	}
	
	public void setTenantDBConnData(TenantDBConnectionData sliceDBConnData) {
		this.tenantDBConnData = sliceDBConnData;		
	}
	
	public DBConnectionPool getDBConnectionPool(DBType dbType) {
		DBConnectionPool pool = null;
		switch(dbType) {
		case ORACLE:
			pool = OracleDBConnPool.getInstance();
			break;
			
		case MSSQL:
			pool = MSSqlDBConnPool.getInstance();
			break;
			
		case MYSQL:
			pool = MySQLDBConnPool.getInstance();
			break;			
			
		default:
			break;
		}
		
		pool.setTenantDBConnData(tenantDBConnData);
		return pool;
	}

	public DBConnectionPool getAndInitializeDBConnectionPool(DBType dbType) {
		DBConnectionPool pool = getDBConnectionPool(dbType);
		if(null != pool && !pool.isInitialized()) {
			pool.initializePool(dbType);
		}
		return pool;
	}
	
	/**
	 * Returns a borrowed connection back to the pool
	 * @param conn - DB connection object to release
	 */
	protected void returnDBConnection(Connection conn, DBType dbType) {
		try {
			if(null != conn && !conn.isClosed()) {
				DBConnectionPool pool = getDBConnectionPool(dbType);
				//If it is slice db connection, resetting back to shared db
				if(dbType == DBType.ORACLE || dbType == DBType.MSSQL || dbType == DBType.MYSQL) {
					if(null == tenantDBConnData) {
						System.err.println("...WARNING: Returning connection, tenant db conn data set as null. This may not return the connection back to right pool, connection leak will happen.");
					}
					
					conn.setCatalog(connectionConfigData.getDefaultTenantDB(dbType).getDefaultTenantDBName());
				}
				
				if(null != pool && conn instanceof PoolableConnection) {
					GenericObjectPool<PoolableConnection> actualPool = pool.getActualPool();
					actualPool.returnObject((PoolableConnection)conn);

					System.out.println("======= POOL::START ======");
					switch(dbType){
					case ORACLE:
						System.out.println("o: " + actualPool.getBorrowedCount() + "/" + actualPool.getReturnedCount());
						break;
						
					case MSSQL:
						System.out.println("m: " + actualPool.getBorrowedCount() + "/" + actualPool.getReturnedCount());
						break;
						
					default:
						break;
					}					
					System.out.println("======== POOL::END =======");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This will fetch first 3 records for slice databases. It will use the db user, password information to
	 * create connection to shared db and create the connection pool.
	 * @param sliceDBType
	 * @return
	 */
	protected boolean initializeSliceDBPool() {
		try {
			DatabaseUtil dbUtil = new DatabaseUtil();
			HashMap<String, TenantDBConnectionData> tenantDBMap = dbUtil.getTenantDBsDetails(true);
			Set<String> sliceKeys = tenantDBMap.keySet();
			for (String sliceID : sliceKeys) {
				TenantDBConnectionData sliceConnData = tenantDBMap.get(sliceID);
				DBConnectionPoolFactory connFactory = DBConnectionPoolFactory.getInstance();	
				connFactory.setTenantDBConnData(sliceConnData);
				DBConnectionPool connPool = connFactory.getAndInitializeDBConnectionPool(sliceConnData.getDbType());
				PoolableConnection pConn = connPool.getActualPool().borrowObject();
				if(null != pConn){
					connPool.getActualPool().returnObject(pConn);
					return true;
				}
			}		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return false;
	}

	private void initializeSQLDriver() {
		List<DefaultDatabase> listDummyDBs = connectionConfigData.getDefaultDatabases();
		if (null == listDummyDBs) {
			listDummyDBs = new ArrayList<>();
		}
		
		for (DefaultDatabase defaultDatabase : listDummyDBs) {
			boolean bReturn = DBConnectionPool.initializeDriver(defaultDatabase);
			if(bReturn) {
				System.out.println("Initialized SQL JDBC driver " + connectionConfigData.getDefaultTenantDB(defaultDatabase.getDbType()).getDriverClassName() + " successfully.");
			}	
		}
	}	
}
