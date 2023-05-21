package org.nextlevel.es.tenants;

import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="tenants")
public class TenantsConfiguration {
	private static Logger logger = LoggerFactory.getLogger(TenantsConfiguration.class);	

	private ArrayList<Tenant> allTenants;
	private HashMap<String, Tenant> mapTenants = new HashMap<String, Tenant>();
	
	public ArrayList<Tenant> getAllTenants() {
		populateMapOfTenants();
		return allTenants;
	}

	@XmlElement (name="tenantInstance")	
	public void setAllTenants(ArrayList<Tenant> allTenants) {
		this.allTenants = allTenants;
		populateMapOfTenants();
	}

	public Tenant getTenant(String tenantID) {
		return mapTenants.get(tenantID);
	}	
	
	private void populateMapOfTenants() {
		if(null != allTenants && allTenants.size() > 0) {
			if(mapTenants.size() == 0) {
				for (Tenant tenant : allTenants) {
					if(!mapTenants.containsKey(tenant.getTenantId())) {
						mapTenants.put(tenant.getTenantId(), tenant);
					} else {
						logger.error("Duplicate tenant id <" + tenant.getTenantId() + "> found!" );
					}
				}
			}
		}
	}
}
