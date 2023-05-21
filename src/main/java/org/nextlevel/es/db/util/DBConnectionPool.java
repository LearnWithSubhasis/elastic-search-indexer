package org.nextlevel.es.db.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.conn.ConnectionData;
import org.nextlevel.es.conn.ConnectionReader;
import org.nextlevel.es.conn.DefaultDatabase;

/**
 * The abstract class implementation for DB connection pool
 * 
 * @author nextlevel
 *
 */
public abstract class DBConnectionPool {
	
	private static Logger logger = LoggerFactory.getLogger(DBConnectionPool.class);	
	
	protected static ConnectionReader connReader = ConnectionReader.getInstance();
	protected static ConnectionData connData = connReader.getConnectionData();
	protected PoolingDataSource<PoolableConnection> dataSource = null;
	private GenericObjectPool<PoolableConnection> connectionPool = null;
	protected Map<String, GenericObjectPool<PoolableConnection>> mapConnectionPool = new HashMap<String, GenericObjectPool<PoolableConnection>>();
	protected TenantDBConnectionData tenantDBConnData;

	protected String databaseServerName = null;
	protected String databaseServerPort = null;
	protected boolean isDataSourceInitialized = false;
	protected DBType dbType = DBType.NONE;
	
	protected DBConnectionPool(){
		connData = connReader.getConnectionData();
	}
    
    public static boolean initializeDriver(DefaultDatabase defaultDatabase) {
        //
        // First we load the underlying JDBC driver.
        // You need this if you don't use the jdbc.drivers
        // system property.
        //
    	logger.info("Loading underlying JDBC driver.");
        System.out.println("Loading underlying JDBC driver.");
        try {
            Class.forName(defaultDatabase.getDriverClassName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        
        logger.info("Done.");
        System.out.println("Done.");
        return true;
    }
    
	protected String getFormattedConnectionURL(String connectionUnformattedURL,
			String databaseServerName, String databaseServerPort, String databaseSchemaName) {
	      return MessageFormat.format (connectionUnformattedURL, databaseServerName, databaseServerPort, databaseSchemaName);
	}

	protected PoolingDataSource<PoolableConnection> setupDataSource(DBType dbType2) {
		this.dbType = dbType2;
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory =
            new DriverManagerConnectionFactory(getConnectionURL(), getDBUser(), getDBPassword());

        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
            new PoolableConnectionFactory(connectionFactory, null);

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(connData.getMaxIdle());
        config.setMaxTotal(connData.getMaxActive());
        config.setMaxWaitMillis(connData.getMaxWait());
        config.setTestOnBorrow(connData.isTestOnBorrow());
        config.setMinIdle(connData.getMaxIdle());

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory, config);
        
        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);
        
        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        dataSource = new PoolingDataSource<>(connectionPool);

        isDataSourceInitialized = true;
//        if(dbType == DBType.ORACLE) {
//        	mapConnectionPool.put(getTenantDBConnData().getDbServer(), connectionPool);
//        }
        mapConnectionPool.put(getTenantDBConnData().getDbServer(), connectionPool);
        return dataSource;
    }	
	
	protected abstract String getConnectionURL();

	public PoolingDataSource<PoolableConnection> getDataSource() {
		return dataSource;
	}
	
	protected String getDBPassword() {
		return connData.getDefaultTenantDB(dbType).getPassword();
	}

	protected String getDBUser() {
		return connData.getDefaultTenantDB(dbType).getUsername();
	}

	protected void initializePool(DBType dbType2) {
		if(!isInitialized()) {
			dataSource = setupDataSource(dbType2);
		}
	}
	
	public boolean isInitialized() {
		if(dbType == DBType.ORACLE || dbType == DBType.MSSQL || dbType == DBType.MYSQL) {
			if(isDataSourceInitialized && (null != tenantDBConnData) 
					&& (mapConnectionPool.containsKey(tenantDBConnData.getDbServer())) 
					&& (null != mapConnectionPool.get(tenantDBConnData.getDbServer()))) {
				return true;
			}
		}
		
		return false;
	}

	protected GenericObjectPool<PoolableConnection> getActualPool() {
		if(dbType == DBType.ORACLE || dbType == DBType.MSSQL || dbType == DBType.MYSQL) {
			if(isDataSourceInitialized && (null != tenantDBConnData) 
					&& (mapConnectionPool.containsKey(tenantDBConnData.getDbServer()))) {
				return mapConnectionPool.get(tenantDBConnData.getDbServer());
			}
		}		
		return connectionPool;
	}
	
	
	public abstract TenantDBConnectionData getTenantDBConnData();
	public abstract void setTenantDBConnData(TenantDBConnectionData tenantDBConnData);
 }


