package org.nextlevel.es.tc;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.nextlevel.es.common.ConfigUtil;

/**
 * 
 * @author nextlevel
 *
 */
public class ElasticSearchConfigurationReader {
	private static final String ELASTICSEARCH_SERVER_CONFIG_XML = "elasticsearch-server-config.xml";
	private static ElasticSearchConfiguration serverConfig = null;
	public static ElasticSearchConfigurationReader config = new ElasticSearchConfigurationReader();
	
	private ElasticSearchConfigurationReader(){
		initialize();
	}
	
	public static ElasticSearchConfigurationReader getInstance() {
		synchronized (config) {
			if(null == config) {
				config = new ElasticSearchConfigurationReader();
				initialize();
			}
		}
		
		return config;
	}
	
	private static void initialize() {
		if(null == serverConfig) {
			try {
				InputStream configFileStream = ConfigUtil.getInstance().getElasticSearchConfiguration(ELASTICSEARCH_SERVER_CONFIG_XML);			
				JAXBContext jaxbContext = JAXBContext.newInstance(ElasticSearchConfiguration.class);
		
				Unmarshaller jaxbUnmarshaller;
					jaxbUnmarshaller = jaxbContext.createUnmarshaller();
					serverConfig = (ElasticSearchConfiguration) jaxbUnmarshaller.unmarshal(configFileStream);
			} catch (JAXBException e) {
				e.printStackTrace();
			}
		}
	}

	public ElasticSearchConfiguration getConfiguration() {
		return serverConfig;
	}
	
	public static void main(String[] args) {
		ElasticSearchConfigurationReader reader = ElasticSearchConfigurationReader.getInstance();
		ElasticSearchConfiguration config = reader.getConfiguration();
		if(null != config) {
			System.out.println(config.getClusterName());
			System.out.println(config.getServerHosts().get(0).getNodeName());
			System.out.println(config.getServerHosts().get(0).getNodeType());
			System.out.println(config.getTransportClientPort());
			System.out.println(config.isClientTransportSniff());
		}
		
//		ElasticSearchConfiguration config = new ElasticSearchConfiguration();
//		ServerHost sh1 = new ServerHost();
//		sh1.setNodeName("nextlevel-w2k8-2");
//		sh1.setNodeType("Index");
//		config.setClientTransportSniff(false);
//		config.setClusterName("csm-cluster");
//		config.setTransportClientPort(9300);
//		config.setHttpPort(9200);
//		ArrayList<ServerHost> listHosts = new ArrayList<ServerHost>();
//		listHosts.add(sh1);
//		config.setServerHosts(listHosts);
//
//		JAXBContext jaxbContext;
//		try {
//			jaxbContext = JAXBContext.newInstance(ElasticSearchConfiguration.class);
//			Marshaller jaxbMarshaller;
//			jaxbMarshaller = jaxbContext.createMarshaller();
//			jaxbMarshaller.marshal(config, System.out);
//		} catch (JAXBException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}	
}
