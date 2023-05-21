package org.nextlevel.es.mq;

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
public class MQConfigReader {
	
	private static final String MQ_CONFIG_XML = "messagebus-config.xml";
	private static MQConfigReader instance = new MQConfigReader();
	private MQConfig mqConfig = null; 
	static {
		LogInitializer.initializeLogger();
	}
	
	private MQConfigReader(){
		initializeConfiguration();
	}
	
	public static MQConfigReader getInstance() {
		if(null == instance) {
			instance = new MQConfigReader();
		}
		return instance;
	}
	
	protected MQConfig initializeConfiguration() {
		if(null == mqConfig) {
			try {
				InputStream configFile = ConfigUtil.getInstance().getMQConfiguration(MQ_CONFIG_XML);			
				JAXBContext jaxbContext = JAXBContext.newInstance(MQConfig.class, RabbitMQConfig.class);
		
				Unmarshaller jaxbUnmarshaller;
					jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				mqConfig = (MQConfig) jaxbUnmarshaller.unmarshal(configFile);
			} catch (JAXBException e) {
				e.printStackTrace();
			} finally {
			}
		}
		
		return mqConfig;
	}
	
	public static void main(String[] args) {
		MQConfigReader reader = MQConfigReader.getInstance();
		MQConfig config = reader.getMQConfiguration();
		if(null != config) {
			System.out.println("MQ Type: " + config.getMessageBusType());
			System.out.println("MQ RealTime Queue: " + config.getRealTimeQueueName());
			
			if (config.getMessageBusType().equalsIgnoreCase("RabbitMQ")) { //TODO:: convert to enum
				RabbitMQConfig messageBus = config.getRabbitMQConfig();
				if(null != messageBus) {
					System.out.println("Host: " + messageBus.getHost());
				}
			} else if (config.getMessageBusType().equalsIgnoreCase("Kafka")) { //TODO:: convert to enum
				KafkaConfig messageBus = config.getKafkaConfig();
				if(null != messageBus) {
					System.out.println("Host: " + messageBus.getHost());
				}
			}
		}
	}
	
	public MQConfig getMQConfiguration() {
		return mqConfig;
	}
}
