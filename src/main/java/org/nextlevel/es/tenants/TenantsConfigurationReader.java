package org.nextlevel.es.tenants;

import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.nextlevel.es.common.ConfigUtil;

/**
 * 
 * @author nextlevel
 *
 */
public class TenantsConfigurationReader {
	private static final String DATACENTER_ALL_TENANTS_XML = "dc-all-tenants.xml";
	private static TenantsConfiguration tenantsConfig = null;
	public static TenantsConfigurationReader config = new TenantsConfigurationReader();
	
	private TenantsConfigurationReader(){
		initialize();
	}
	
	public static TenantsConfigurationReader getInstance() {
		synchronized (config) {
			if(null == config) {
				config = new TenantsConfigurationReader();
				initialize();
			}
		}
		
		return config;
	}
	
	private static void initialize() {
		if(null == tenantsConfig) {
			try {
				InputStream configFileStream = ConfigUtil.getInstance().getTenantsConfiguration(DATACENTER_ALL_TENANTS_XML);			
				JAXBContext jaxbContext = JAXBContext.newInstance(TenantsConfiguration.class);
		
				Unmarshaller jaxbUnmarshaller;
					jaxbUnmarshaller = jaxbContext.createUnmarshaller();
					tenantsConfig = (TenantsConfiguration) jaxbUnmarshaller.unmarshal(configFileStream);
			} catch (JAXBException e) {
				e.printStackTrace();
			}
		}
	}

	public TenantsConfiguration getConfiguration() {
		return tenantsConfig;
	}
	
	public static void main(String[] args) {
		TenantsConfigurationReader reader = TenantsConfigurationReader.getInstance();
		TenantsConfiguration config = reader.getConfiguration();
		if(null != config) {
			List<Tenant> allTenants = config.getAllTenants();
			for (Tenant tenant : allTenants) {
				TenantDatabaseInstance tenantDB = tenant.getTenantDBInstance();
				System.out.println(tenant.getTenantId() + ", " + tenantDB.getDbHost() + ", " + tenantDB.getSchemaName() + ", " + tenantDB.getUsername());
			}
		}
	}	
}
