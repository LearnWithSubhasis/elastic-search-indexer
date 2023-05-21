package org.nextlevel.es.tenants;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="tenantInstance")
public class Tenant {
	private String id;
	private String appId;
	private String bgId;
	private boolean elasticSearchEnabled;

	private TenantDatabaseInstance tenantDBInstance;
	public String getTenantId() {
		return id;
	}
	@XmlAttribute (name="id")
	public void setTenantId(String id) {
		this.id = id;
	}
	public String getAppId() {
		return appId;
	}
	@XmlAttribute (name="appId")
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public TenantDatabaseInstance getTenantDBInstance() {
		return tenantDBInstance;
	}
	@XmlElement (name="tenantDatabaseInstance")
	public void setTenantDBInstance(TenantDatabaseInstance tenantDBInstance) {
		this.tenantDBInstance = tenantDBInstance;
	}
	public String getBgId() {
		return bgId;
	}
	@XmlAttribute (name="bgId")
	public void setBgId(String bgId) {
		this.bgId = bgId;
	}
	public boolean isElasticSearchEnabled() {
		return elasticSearchEnabled;
	}
	@XmlElement (name="elasticSearchEnabled")
	public void setElasticSearchEnabled(boolean elasticSearchEnabled) {
		this.elasticSearchEnabled = elasticSearchEnabled;
	}	
}
