package org.nextlevel.es.db.util;

import org.nextlevel.es.conn.ConnectionData;
import org.nextlevel.es.tenants.Tenant;

/**
 * 
 * @author nextlevel
 *
 */
public class TenantDBConnectionData {
	private String slice;
	private String dbSchemaName;
	private String dbServer;
	private int dbPort;
	private String dbDriver;
	private String dbUser;
	private String dbPassword;
	private DBType dbType;
	public String getDbSchemaName() {
		return dbSchemaName;
	}
	public void setDbSchemaName(String dbSchemaName) {
		this.dbSchemaName = dbSchemaName;
	}
	public String getDbServer() {
		return dbServer;
	}
	public void setDbServer(String dbServer) {
		this.dbServer = dbServer;
	}
	public int getDbPort() {
		return dbPort;
	}
	public void setDbPort(int dbPort) {
		this.dbPort = dbPort;
	}
	public String getDbDriver() {
		return dbDriver;
	}
	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}
	public String getDbUser() {
		return dbUser;
	}
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}
	public String getDbPassword() {
		return dbPassword;
	}
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}
	public String getSlice() {
		return slice;
	}
	public void setSlice(String slice) {
		this.slice = slice;
	}
	public DBType getDbType() {
		return dbType;
	}
	public void setDbType(DBType dbType) {
		this.dbType = dbType;
	}
	
	public static TenantDBConnectionData copyFrom(Tenant tenant, ConnectionData connectionData) {
		TenantDBConnectionData data = new TenantDBConnectionData();
		data.setSlice(tenant.getTenantId());
		data.setDbServer(tenant.getTenantDBInstance().getDbHost());
		data.setDbType(tenant.getTenantDBInstance().getDbType());
		data.setDbDriver(connectionData.getDBDriverClass(tenant.getTenantDBInstance().getDbType()));
		data.setDbPassword(tenant.getTenantDBInstance().getPassword());
		data.setDbPort(tenant.getTenantDBInstance().getPort());
		data.setDbSchemaName(tenant.getTenantDBInstance().getSchemaName());
		data.setDbServer(tenant.getTenantDBInstance().getDbHost());
		data.setDbUser(tenant.getTenantDBInstance().getUsername());
		
		return data;
	}
	
	public String toString() {
		return new StringBuffer("Slice DB Connection Data: ").append("DB Server: ").append(dbServer).append(", DB Schema: ").append(dbSchemaName)
				.toString();
	}

}
