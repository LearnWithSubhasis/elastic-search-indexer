//package org.nextlevel.es.mq;
//
//public class MQFactory {
//	private static MQFactory instance = new MQFactory();
//	private MQFactory(){}
//	
//	public static MQFactory getInstance(){
//		return instance;
//	}
//	
//	public IMessageBus getMessageBus(String busType) {
//		switch(busType) {
//		case "RabbitMQ":
//			return new RabbitMQConfig();
//			
//			default:
//				break;
//		}
//		
//		return null;
//	}
//}
