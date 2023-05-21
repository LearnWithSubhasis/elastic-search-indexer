package org.nextlevel.es.tenants;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.db.util.DBType;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="tenantDatabaseInstance")
public class TenantDatabaseInstance {
	private static Logger logger = LoggerFactory.getLogger(TenantDatabaseInstance.class);	

	private String id;
	private String serviceId;
	private String sid;
	private String serviceName;
	private String dbHost;
	private String dbId;
	private String vendor;
	private String username;
	private String password;
	private String schemaName;
	private int port;
	private DBType dbType;
	
	public String getServiceName() {
		return serviceName;
	}
	@XmlAttribute (name="serviceName")
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	public String getDbHost() {
		return dbHost;
	}
	@XmlAttribute (name="dbHost")
	public void setDbHost(String dbHost) {
		this.dbHost = dbHost;
	}
	public String getDbId() {
		return dbId;
	}
	@XmlAttribute (name="dbId")
	public void setDbId(String dbId) {
		this.dbId = dbId;
	}
	public String getVendor() {
		return vendor;
	}
	@XmlAttribute (name="vendor")
	public void setVendor(String vendor) {
		this.vendor = vendor;
	}
	public String getUsername() {
		return username;
	}
	@XmlAttribute (name="username")
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	@XmlAttribute (name="password")
	public void setPassword(String password) {
		this.password = password;
	}
	public String getSchemaName() {
		return schemaName;
	}
	@XmlAttribute (name="schemaName")
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public int getPort() {
		return port;
	}
	@XmlAttribute (name="port")
	public void setPort(int port) {
		this.port = port;
	}
	public String getId() {
		return id;
	}
	@XmlAttribute (name="id")
	public void setId(String id) {
		this.id = id;
	}
	public String getServiceId() {
		return serviceId;
	}
	@XmlAttribute (name="serviceId")
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	public String getSid() {
		return sid;
	}
	@XmlAttribute (name="sid")
	public void setSid(String sid) {
		this.sid = sid;
	}
	public DBType getDbType() {
		dbType = DBType.valueOf(vendor.toUpperCase());
		logger.info("DB Type evaluated to: " + dbType.name());
		
		return dbType;
	}
}
