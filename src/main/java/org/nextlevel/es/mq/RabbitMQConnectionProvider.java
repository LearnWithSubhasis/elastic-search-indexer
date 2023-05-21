package org.nextlevel.es.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import org.json.JSONObject;
import org.nextlevel.es.IndexManager;
import org.nextlevel.es.IndexManagerFactory;
import org.nextlevel.es.IndexOperationMethod;
import org.nextlevel.es.IndexOperationType;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitMQConnectionProvider {
	private static RabbitMQConnectionProvider instance = new RabbitMQConnectionProvider();
	private MQConfig mqConfig = MQConfigReader.getInstance().getMQConfiguration();
	private static String ES_REALTIME_METADATA_QUEUE_NAME;

	private Connection connection;
	private Channel channel;
	private RabbitMQConnectionProvider(){
		ES_REALTIME_METADATA_QUEUE_NAME = mqConfig.getRealTimeQueueName();
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(mqConfig.getRabbitMQConfig().getHost());
		factory.setPort(mqConfig.getRabbitMQConfig().getPort());
		try {
			factory.setVirtualHost(mqConfig.getRabbitMQConfig().getVirtualHost());
			factory.setUsername(mqConfig.getRabbitMQConfig().getUser());
			factory.setPassword(mqConfig.getRabbitMQConfig().getPassword());
			setConnection(factory.newConnection());
			setChannel(getConnection().createChannel());
			channel.queueDeclare(ES_REALTIME_METADATA_QUEUE_NAME, false, false, false, null);
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void subscribeForIndexingRequests(Consumer consumerSpecific) throws IOException {
		Consumer consumer = null;
		if(null == consumerSpecific) {
			consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag,
						Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					String message = new String(body, "UTF-8");
					System.out.println(" [Realtime Indexer] Received '" + message + "'");
					
					JSONObject jsonObj = new JSONObject(message);
					invokeNearRealTimeIndexing(jsonObj);
				}
			};
		} else {
			consumer = consumerSpecific; 
		}
		
		channel.basicConsume(ES_REALTIME_METADATA_QUEUE_NAME, true, consumer);
	}
	
	protected void invokeNearRealTimeIndexing(JSONObject jsonObj) {
		IndexManagerFactory indexManagerFactory = IndexManagerFactory.getInstance();
		indexManagerFactory.setIndexOperationType(IndexOperationType.Selective);
		
		IndexManager indexManager = indexManagerFactory.getIndexManager();
		indexManager.setIndexDocumentType(jsonObj.getString("object_code"));
		indexManager.setTenantID(jsonObj.getString("tenant_id"));
		
		String eventType = jsonObj.getString("operation_type");
		switch(eventType) {
		case "CREATE":
			indexManager.setIndexOperationMethod(IndexOperationMethod.Create);
			break;
			
		case "UPDATE":
		case "ACCESS_CHANGED_INSTANCE":
		case "ACCESS_CHANGED_OBS":
		case "ACCESS_CHANGED_GROUP":
			indexManager.setIndexOperationMethod(IndexOperationMethod.Update);
			break;
	
		case "DELETE":
			indexManager.setIndexOperationMethod(IndexOperationMethod.Delete);
			break;
		}
		
		ArrayList<Long> idsToIndex = new ArrayList<Long>();
		idsToIndex.add(jsonObj.getLong("object_id"));
		
		indexManager.setSelectedIDsToIndex(idsToIndex);
		indexManager.index();		
	}

	public Connection getConnection() {
		return connection;
	}
	private void setConnection(Connection connection) {
		this.connection = connection;
	}
	public Channel getChannel() {
		return channel;
	}
	private void setChannel(Channel channel) {
		this.channel = channel;
	}
	
	public static RabbitMQConnectionProvider getInstance() {
		return instance;
	}
}
