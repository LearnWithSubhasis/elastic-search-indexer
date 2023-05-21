package org.nextlevel.es.mq;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;

import org.json.JSONObject;
import org.nextlevel.es.IndexManager;
import org.nextlevel.es.IndexManagerFactory;
import org.nextlevel.es.IndexOperationMethod;
import org.nextlevel.es.IndexOperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaConnectionProvider {
	private static final Logger log = LoggerFactory.getLogger(KafkaConnectionProvider.class);

	private static KafkaConnectionProvider instance = new KafkaConnectionProvider();
	private MQConfig mqConfig = MQConfigReader.getInstance().getMQConfiguration();
	private static String ES_REALTIME_METADATA_QUEUE_NAME;
	
	private KafkaConsumer<String, String> consumer = null;
	private Properties props = new Properties();

	private KafkaConnectionProvider(){
		ES_REALTIME_METADATA_QUEUE_NAME = mqConfig.getRealTimeQueueName();
		
		try {
		      props.put("bootstrap.servers", "localhost:9092");
		      props.put("group.id", "test");
		      props.put("enable.auto.commit", "true");
		      props.put("auto.commit.interval.ms", "1000");
		      props.put("session.timeout.ms", "30000");
		      props.put("key.deserializer", 
		         "org.apache.kafka.common.serialization.StringDeserializer");
		      props.put("value.deserializer", 
		         "org.apache.kafka.common.serialization.StringDeserializer");
		      consumer = new KafkaConsumer<String, String>(props);
		      
		      //Kafka Consumer subscribes list of topics here.
		      consumer.subscribe(Arrays.asList(ES_REALTIME_METADATA_QUEUE_NAME));
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void subscribeForIndexingRequests(Consumer consumerSpecific) throws Exception {
		Consumer consumer = null;
		if(null == consumerSpecific) {
			consumer = new KafkaConsumer<String, String>(props);		      
		} else {
			consumer = consumerSpecific; 
		}
		
	    //Kafka Consumer subscribes list of topics here.
	    consumer.subscribe(Arrays.asList(ES_REALTIME_METADATA_QUEUE_NAME));
	    
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(10);
			try {
				for (ConsumerRecord<String, String> record : records) {
	
					// print the offset,key and value for the consumer records.
					System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
	
					String message = record.value();
					System.out.println(" [Realtime Indexer] Received '" + message + "'");
					
					JSONObject jsonObj = new JSONObject(message);
					invokeNearRealTimeIndexing(jsonObj);				
				}
			} catch (Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
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

	public KafkaConsumer<String, String> getConsumer() {
		return consumer;
	}
	
	private void setConsumer(Consumer<String, String> consumer) {
		this.consumer = (KafkaConsumer<String, String>) consumer;
	}
	
	public static KafkaConnectionProvider getInstance() {
		return instance;
	}
}
