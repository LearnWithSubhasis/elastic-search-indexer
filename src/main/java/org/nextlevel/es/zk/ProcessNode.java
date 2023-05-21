package org.nextlevel.es.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.nextlevel.es.IndexManager;
import org.nextlevel.es.IndexManagerFactory;
import org.nextlevel.es.IndexOperationMethod;
import org.nextlevel.es.IndexOperationType;
import org.nextlevel.es.mq.KafkaConfig;
import org.nextlevel.es.mq.KafkaConnectionProvider;
import org.nextlevel.es.mq.MQConfig;
import org.nextlevel.es.mq.MQConfigReader;
import org.nextlevel.es.mq.RabbitMQConfig;
import org.nextlevel.es.mq.RabbitMQConnectionProvider;
import org.nextlevel.es.zk.config.ZooKeeperConfig;
import org.nextlevel.es.zk.config.ZooKeeperConfigReader;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 
 * @author subhasiskhatua
 *
 */
public class ProcessNode implements Runnable {
	
	private static final Logger LOG = Logger.getLogger(ProcessNode.class);
	
	private static String LEADER_ELECTION_ROOT_NODE;
	private static String PROCESS_NODE_PREFIX;
	
	private final int processId;
	private final ZKService zooKeeperService;
	private final ZooKeeper zooKeeper;
	
	private String processNodePath;
	private String watchedNodePath;
	
    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;
    
	private ZooKeeperConfig zkConfig = ZooKeeperConfigReader.getInstance().getZookeeperConfiguration();	
	
	public ProcessNode(final int id, final String zkURL) throws IOException {
		LEADER_ELECTION_ROOT_NODE = zkConfig.getPathForElection();
		PROCESS_NODE_PREFIX = zkConfig.getProcessNodePrefix();
		
		this.processId = id;
		zooKeeperService = new ZKService(zkURL, new ProcessNodeWatcher());
		zooKeeper = zooKeeperService.getZooKeeper();
		
        this.executor = new ThreadPoolExecutor(1, 1, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
	}
	
	private void attemptForLeaderPosition() {
		
		final List<String> childNodePaths = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, false);
		
		Collections.sort(childNodePaths);
		
		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
		if(index == 0) {
			if(LOG.isInfoEnabled()) {
				LOG.info("[Process: " + processId + "] I am the new leader!");
				System.out.println("[Process: " + processId + "] I am the new leader!");
				zooKeeperService.bootstrapAsLeader();
				performLeaderRole();
				if(zkConfig.isMasterCanWork()) {
					performWorkerRole();
				}
			}
		} else {
			final String watchedNodeShortPath = childNodePaths.get(index - 1);
			
			watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;
			
			if(LOG.isInfoEnabled()) {
				LOG.info("[Process: " + processId + "] - Setting watch on node with path: " + watchedNodePath);
				System.out.println("[Process: " + processId + "] - Setting watch on node with path: " + watchedNodePath);
			}
			zooKeeperService.watchNode(watchedNodePath, true);
			performWorkerRole();
		}
	}


    
	private void performWorkerRole() {
		registerAsWorker(processId);
		zooKeeperService.createParent("/nextlevel/es/indexer/assign/" + "worker-" + processId, new byte[0]);
		getTasks();		
	}

	private void performLeaderRole() {
		//subscribe to message bus
		try {
			MQConfigReader mqConfigReader = MQConfigReader.getInstance();
			MQConfig config = mqConfigReader.getMQConfiguration();
			if(null != config) {
				System.out.println("MQ Type: " + config.getMessageBusType());
				System.out.println("MQ RealTime Queue: " + config.getRealTimeQueueName());
				
				if (config.getMessageBusType().equalsIgnoreCase("RabbitMQ")) { //TODO:: convert to enum
					RabbitMQConnectionProvider mqConnProvider = RabbitMQConnectionProvider.getInstance();
					mqConnProvider.subscribeForIndexingRequests(new DefaultConsumer(mqConnProvider.getChannel()){
						@Override
						public void handleDelivery(String consumerTag,
								Envelope envelope, AMQP.BasicProperties properties,
								byte[] body) throws IOException {
							String message = new String(body, "UTF-8");
							System.out.println(" [Realtime Indexer] Received '" + message + "'");
							
							JSONObject jsonObj = new JSONObject(message);
							System.out.println(jsonObj);
							
							try {
								createTask(jsonObj);
							} catch (Exception ex) {
								States states = zooKeeper.getState();
								System.out.println("ZooKeeper connection may have been lost: "+ states.name());
							}
						}
					});
				} else if (config.getMessageBusType().equalsIgnoreCase("Kafka")) { //TODO:: convert to enum
					KafkaConfig messageBus = config.getKafkaConfig();
					if(null != messageBus) {
						System.out.println("Host: " + messageBus.getHost());
					}
					
					KafkaConnectionProvider mqConnProvider = KafkaConnectionProvider.getInstance();
					KafkaConsumer<String, String> consumer = mqConnProvider.getConsumer();
					mqConnProvider.subscribeForIndexingRequests(consumer);					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to perform operations in leader role: "+ e.getMessage());
		}
	}

	protected void createTask(JSONObject jsonObj) {
		zooKeeperService.createTaskNode(jsonObj);
	}

	@Override
	public void run() {
		
		if(LOG.isInfoEnabled()) {
			LOG.info("Process with id: " + processId + " has started!");
			System.out.println("Process with id: " + processId + " has started!");
		}
		
		final String rootNodePath = zooKeeperService.createNode(LEADER_ELECTION_ROOT_NODE, false, false);
		if(rootNodePath == null) {
			throw new IllegalStateException("Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		
		processNodePath = zooKeeperService.createNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
		if(processNodePath == null) {
			throw new IllegalStateException("Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("[Process: " + processId + "] Process node created with path: " + processNodePath);
			System.out.println("[Process: " + processId + "] Process node created with path: " + processNodePath);
		}

		attemptForLeaderPosition();
	}
	
	public class ProcessNodeWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("[Process: " + processId + "] Event received: " + event);
			}
			
			final EventType eventType = event.getType();
			if(EventType.NodeDeleted.equals(eventType)) {
				if(event.getPath().equalsIgnoreCase(watchedNodePath)) {
					attemptForLeaderPosition();
				}
			}
			
		}		
	}
	
    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /nextlevel/es/indexer/workers.
     */
	public void registerAsWorker(int processId) {
        processName = "worker-" + processId;
        zooKeeper.create("/nextlevel/es/indexer/workers/" + processName,
                "Idle".getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
	}
    
    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
            	registerAsWorker(processId);
                
                break;
            case OK:
                LOG.info("Registered successfully: " + processId);
                
                break;
            case NODEEXISTS:
                LOG.warn("Already registered: " + processId);
                
                break;
            default:
                LOG.error("Something went wrong: ", 
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };   
    
    /*
     *************************************** 
     ***************************************
     * Methods to wait for new assignments.*
     *************************************** 
     ***************************************
     */
    
    Watcher newTaskWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert new String("/nextlevel/es/indexer/assign/worker-"+ processId ).equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
        zooKeeper.getChildren("/nextlevel/es/indexer/assign/worker-" + processId, 
                newTaskWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                updateStatus((String)ctx);
                return;
            }
        }
    };

    String status;
    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zooKeeper.setData("/nextlevel/es/indexer/workers/" + processName, status.getBytes(), -1,
                statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }
    
    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }
    
    protected WorkersCache assignedTasksCache = new WorkersCache();
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null){
                    executor.execute(new Runnable() {
                        List<String> children;
                        DataCallback cb;
                        
                        /*
                         * Initializes input of anonymous class
                         */
                        public Runnable init (List<String> children, DataCallback cb) {
                            this.children = children;
                            this.cb = cb;
                            
                            return this;
                        }
                        
                        public void run() {
                            if(children == null) {
                                return;
                            }
    
                            LOG.info("Looping into tasks");
                            setStatus("Working");
                            for(String task : children){
                                LOG.debug("New task: " + task);
                                zooKeeper.getData("/nextlevel/es/indexer/assign/worker-" + processId  + "/" + task,
                                        false,
                                        cb,
                                        task);   
                            }
                        }
                    }.init(assignedTasksCache.addedWorkersToCache(children), taskDataCallback));
                } 
                break;
            default:
                System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zooKeeper.getData(path, false, taskDataCallback, null);
                break;
            case OK:
                /*
                 *  Executing a task in this example is simply printing out
                 *  some string representing the task.
                 */
                executor.execute( new Runnable() {
                    byte[] data;
                    Object ctx;
                    
                    /*
                     * Initializes the variables this anonymous class needs
                     */
                    public Runnable init(byte[] data, Object ctx) {
                        this.data = data;
                        this.ctx = ctx;
                        
                        return this;
                    }
                    
                    public void run() {
                    	String jsonIndexMessage = new String(data);
                        LOG.info("Executing your task: " + jsonIndexMessage);
                        
    					JSONObject jsonObj = new JSONObject(jsonIndexMessage);
    					invokeNearRealTimeIndexing(jsonObj);                        
                        
                        zooKeeper.create("/nextlevel/es/indexer/status/" + (String) ctx, "done".getBytes(), Ids.OPEN_ACL_UNSAFE, 
                                CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                        zooKeeper.delete("/nextlevel/es/indexer/assign/worker-" + processId + "/" + (String) ctx, 
                                -1, taskVoidCallback, null);
                    }
                }.init(data, ctx));
                
                break;
            default:
                LOG.error("Failed to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zooKeeper.create(path + "/nextlevel/es/indexer/status", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        taskStatusCreateCallback, null);
                break;
            case OK:
                LOG.info("Created status znode correctly: " + name);
                break;
            case NODEEXISTS:
                LOG.warn("Node exists: " + path);
                break;
            default:
                LOG.error("Failed to create task data: ", KeeperException.create(Code.get(rc), path));
            }
            
        }
    };
    
    VoidCallback taskVoidCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            } 
        }
    };

	private String processName;    

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
}
