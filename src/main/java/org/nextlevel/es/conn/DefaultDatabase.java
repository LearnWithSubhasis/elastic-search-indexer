package org.nextlevel.es.conn;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import org.nextlevel.es.db.util.DBType;

public class DefaultDatabase {
	String url;
	String username;
	String password;
	boolean tenantDBsPasswordEncrypted;
	String driverClassName;
	private String dbType;
	private String defaultTenantDBName;

	public String getUrl() {
		return url;
	}
	
	@XmlElement
	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}
	
	@XmlElement
	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getPassword() {
		return password;
	}
	
	@XmlElement
	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isTenantDBsPasswordEncrypted() {
		return tenantDBsPasswordEncrypted;
	}
	
	@XmlElement
	public void setTenantDBsPasswordEncrypted(boolean tenantDBsPasswordEncrypted) {
		this.tenantDBsPasswordEncrypted = tenantDBsPasswordEncrypted;
	}
	
	public String getDriverClassName() {
		return driverClassName;
	}
	
	@XmlElement
	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public String getDatabaseType() {
		return dbType;
	}

	@XmlAttribute (name="dbType")
	public void setDatabaseType(String dbType) {
		this.dbType = dbType;
	}

	public DBType getDbType() {
		return DBType.valueOf(dbType.toUpperCase());
	}

	public String getDefaultTenantDBName() {
		return defaultTenantDBName;
	}

	@XmlElement (name="defaultTenantDB")
	public void setDefaultTenantDBName(String defaultTenantDB) {
		this.defaultTenantDBName = defaultTenantDB;
	}
}
