package org.nextlevel.es.conn;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.nextlevel.es.db.util.DBType;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="ConnectionData")
public class ConnectionData {
	private List<DefaultDatabase> defaultDatabases;
	
	boolean logAbandoned;
	int maxActive;
	int maxIdle;
	long maxWait;
	boolean removeAbandoned;
	int removeAbandonedTimeout;
	boolean testOnBorrow;
	String type;
	String validationQuery;
	
	public boolean isLogAbandoned() {
		return logAbandoned;
	}

	@XmlElement
	public void setLogAbandoned(boolean logAbandoned) {
		this.logAbandoned = logAbandoned;
	}

	public int getMaxActive() {
		return maxActive;
	}

	@XmlElement
	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	@XmlElement
	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public long getMaxWait() {
		return maxWait;
	}

	@XmlElement
	public void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
	}

	public boolean isRemoveAbandoned() {
		return removeAbandoned;
	}

	@XmlElement
	public void setRemoveAbandoned(boolean removeAbandoned) {
		this.removeAbandoned = removeAbandoned;
	}

	public int getRemoveAbandonedTimeout() {
		return removeAbandonedTimeout;
	}

	@XmlElement
	public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
		this.removeAbandonedTimeout = removeAbandonedTimeout;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	@XmlElement
	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public String getType() {
		return type;
	}

	@XmlElement
	public void setType(String type) {
		this.type = type;
	}

	public String getValidationQuery() {
		return validationQuery;
	}

	@XmlElement
	public void setValidationQuery(String validationQuery) {
		this.validationQuery = validationQuery;
	}

	public List<DefaultDatabase> getDefaultDatabases() {
		return defaultDatabases;
	}
	
	@XmlElement (name="database")
	public void setDefaultDatabases(List<DefaultDatabase> defautDatabases) {
		this.defaultDatabases = defautDatabases != null ? defautDatabases : new ArrayList<>();
	}

	public DefaultDatabase getDefaultTenantDB(DBType dbType) {
		if(null != defaultDatabases) {
			for (DefaultDatabase defaultDatabase : defaultDatabases) {
				if(defaultDatabase.getDbType().equals(dbType)) {
					return defaultDatabase;
				}
			}
		}
		
		return null;
	}
	
	public String getDBDriverClass(DBType dbType) {
		if(null != defaultDatabases) {
			for (DefaultDatabase defaultDatabase : defaultDatabases) {
				if(defaultDatabase.getDbType().equals(dbType)) {
					return defaultDatabase.getDriverClassName();
				}
			}
		}
		
		return null;
	}
}
