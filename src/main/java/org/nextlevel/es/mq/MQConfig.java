package org.nextlevel.es.mq;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="MessageBus")
public class MQConfig {
	private String messageBusType;
	private String realTimeQueueName;
	private RabbitMQConfig rabbitMQBusConfig;
	private KafkaConfig kafkaBusConfig;
	
	@XmlAttribute(name="type")
	public String getMessageBusType() {
		return messageBusType;
	}
	public void setMessageBusType(String messageBusType) {
		this.messageBusType = messageBusType;
	}
	@XmlElement(name="RealTimeQueue")
	public String getRealTimeQueueName() {
		return realTimeQueueName;
	}
	public void setRealTimeQueueName(String realTimeQueueName) {
		this.realTimeQueueName = realTimeQueueName;
	}
	
	public RabbitMQConfig getRabbitMQConfig() {
		return rabbitMQBusConfig;
	}
	
	public KafkaConfig getKafkaConfig() {
		return kafkaBusConfig;
	}
	
	@XmlElement(name="RabbitMQ", type=RabbitMQConfig.class)
	public void setActualBusConfigRabbitMQ(RabbitMQConfig rabbitMQBusConfig) {
		this.rabbitMQBusConfig = rabbitMQBusConfig;
	}	
	@XmlElement(name="Kafka", type=KafkaConfig.class)
	public void setActualBusConfigKafka(KafkaConfig kafkaBusConfig) {
		this.kafkaBusConfig = kafkaBusConfig;
	}	
}
