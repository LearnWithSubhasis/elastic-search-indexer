package org.nextlevel.es.zk.config;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.nextlevel.es.common.ConfigUtil;
import org.nextlevel.es.common.LogInitializer;

/**
 * 
 * @author nextlevel
 *
 */
public class ZooKeeperConfigReader {
	
	private static final String ZK_CONFIG_XML = "zookeeper-config.xml";
	private static ZooKeeperConfigReader instance = new ZooKeeperConfigReader();
	private ZooKeeperConfig zkConfig = null; 
	static {
		LogInitializer.initializeLogger();
	}
	
	private ZooKeeperConfigReader(){
		initializeConfiguration();
	}
	
	public static ZooKeeperConfigReader getInstance() {
		if(null == instance) {
			instance = new ZooKeeperConfigReader();
		}
		return instance;
	}
	
	protected ZooKeeperConfig initializeConfiguration() {
		if(null == zkConfig) {
			try {
				InputStream configFile = ConfigUtil.getInstance().getZooKeeperConfiguration(ZK_CONFIG_XML);			
				JAXBContext jaxbContext = JAXBContext.newInstance(ZooKeeperConfig.class);
		
				Unmarshaller jaxbUnmarshaller;
					jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				zkConfig = (ZooKeeperConfig) jaxbUnmarshaller.unmarshal(configFile);
			} catch (JAXBException e) {
				e.printStackTrace();
			} finally {
			}
		}
		
		return zkConfig;
	}
	
	public static void main(String[] args) {
		ZooKeeperConfigReader reader = ZooKeeperConfigReader.getInstance();
		ZooKeeperConfig config = reader.getZookeeperConfiguration();
		if(null != config) {
			System.out.println("ZooKeeper Host: " + config.getZkHostName());
			System.out.println("ZooKeeper Port: " + config.getZkPortNumber());
			System.out.println("ZooKeeper Election Path: " + config.getPathForElection());
		}
	}
	
	public ZooKeeperConfig getZookeeperConfiguration() {
		return zkConfig;
	}
}
