package org.nextlevel.es.mq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQTester {
	private final static String ES_REALTIME_METADATA_QUEUE_NAME = "NEXTLEVEL_ES_REALTIME_METADATA_QUEUE_NAME";

	private Connection connection;
	private Channel channel;

	public static void main(String[] args) {
		RabbitMQTester rmqt = new RabbitMQTester();
		rmqt.initialize();
		rmqt.send();
	}

	private void initialize() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("rabbit-mq-host");
		factory.setPort(5672);
		try {
			factory.setVirtualHost("nextlevel");
			factory.setUsername("test");
			factory.setPassword("test");
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}		
	}

	private void send() {
		try {
			String message = "Subhasis's first message to RabbitMQ";
			channel.queueDeclare(ES_REALTIME_METADATA_QUEUE_NAME, false, false, false, null);
			channel.basicPublish("", ES_REALTIME_METADATA_QUEUE_NAME, null, message.getBytes());
			System.out.println(" [Subhasis] Sent '" + message + "'");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
