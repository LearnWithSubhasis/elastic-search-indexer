package org.nextlevel.es.conn;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.nextlevel.es.common.ConfigUtil;
import org.nextlevel.es.db.util.DBType;

/**
 * 
 * @author nextlevel
 *
 */
public class ConnectionReader {
	private static final String DATABASE_CONFIG_XML = "common-db.xml";
	private static ConnectionReader instance = new ConnectionReader();
	private ConnectionData connectionData = null;
	
	private ConnectionReader(){
		if(null == connectionData) {
			try {
				InputStream connectionFile = ConfigUtil.getInstance().getPlatformDBConfiguration(DATABASE_CONFIG_XML);			
				JAXBContext jaxbContext = JAXBContext.newInstance(ConnectionData.class);
		
				Unmarshaller jaxbUnmarshaller;
					jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				connectionData = (ConnectionData) jaxbUnmarshaller.unmarshal(connectionFile);
			} catch (JAXBException e) {
				e.printStackTrace();
			} 
		}
	}
	
	public static ConnectionReader getInstance() {
		return instance;
	}
	
	public static void main(String[] args) {
		ConnectionReader provider = new ConnectionReader();
		System.out.println(provider.getConnectionData().getDefaultTenantDB(DBType.ORACLE).getUrl());
		System.out.println(provider.getConnectionData().getValidationQuery());
	}

	public ConnectionData getConnectionData() {
		return connectionData;
	}
}
