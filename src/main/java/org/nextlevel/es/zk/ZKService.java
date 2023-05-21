package org.nextlevel.es.zk;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.zk.ProcessNode.ProcessNodeWatcher;
import org.nextlevel.es.zk.RecoveredAssignments.RecoveryCallback;

public class ZKService implements Closeable {
	private static Logger LOG = LoggerFactory.getLogger(ZKService.class);
	private ZooKeeper zooKeeper;
    protected WorkersCache tasksCache;
    protected WorkersCache workersCache;
    private Random random = new Random(this.hashCode());

	public ZKService(final String url, final ProcessNodeWatcher processNodeWatcher) throws IOException {
		zooKeeper = new ZooKeeper(url, 3000, processNodeWatcher);
		bootstrap();
	}

	private void bootstrap() {
		createNode("/nextlevel/es/indexer/tasks", false, false);
	}

	public String createNode(final String node, final boolean watch, final boolean ephimeral) {
		String createdNodePath = null;
		try {
			
			final Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat == null) {
				String allNodesInPath[] = node.split("/");
				StringBuffer nodePathTemp = new StringBuffer("");
				for (String nodePath : allNodesInPath) {
					if(nodePath.trim().length()>0) {
						nodePathTemp.append("/").append(nodePath);
						final Stat nodeStat2 =  zooKeeper.exists(nodePathTemp.toString(), watch);
						if(null == nodeStat2) {
							createdNodePath = zooKeeper.create(nodePathTemp.toString(), new byte[0], Ids.OPEN_ACL_UNSAFE, (ephimeral ?  CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
						}
					}
				}
			} else {
				createdNodePath = node;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return createdNodePath;
	}
	
	public boolean watchNode(final String node, final boolean watch) {
		
		boolean watched = false;
		try {
			final Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat != null) {
				watched = true;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return watched;
	}
	
	public List<String> getChildren(final String node, final boolean watch) {
		
		List<String> childNodes = null;
		
		try {
			childNodes = zooKeeper.getChildren(node, watch);
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return childNodes;
	}

	public void createTaskNode(JSONObject jsonObj) {
		TaskObject taskObj = new TaskObject();
		taskObj.setTask(jsonObj);
        zooKeeper.create("/nextlevel/es/indexer/tasks/task-", 
        		jsonObj.toString().getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,   
                taskObj);		
	}
	
    StringCallback createTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Handling connection loss for a sequential node is a bit
                 * delicate. Executing the ZooKeeper create command again
                 * might lead to duplicate tasks. For now, let's assume
                 * that it is ok to create a duplicate task.
                 */
            	createTaskNode(((TaskObject) ctx).getTask());
                
                break;
            case OK:
                LOG.info("My created task name: " + name);
                watchStatus(name.replace("/tasks/", "/status/"), ctx);
                getTasks();
                
                break;
            default:
                LOG.error("Something went wrong" + KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    protected ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();
    
    void watchStatus(String path, Object ctx){
        ctxMap.put(path, ctx);
        zooKeeper.exists(path, 
                statusWatcher, 
                existsCallback, 
                ctx);
    }
    
    Watcher statusWatcher = new Watcher(){
        public void process(WatchedEvent e){
            if(e.getType() == EventType.NodeCreated) {
                assert e.getPath().contains("/nextlevel/es/indexer/status/task-");
                assert ctxMap.containsKey( e.getPath() );
                
                zooKeeper.getData(e.getPath(), 
                        false, 
                        getDataCallback, 
                        ctxMap.get(e.getPath()));
            }
        }
    };
    
    StatCallback existsCallback = new StatCallback(){
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                watchStatus(path, ctx);
                
                break;
            case OK:
                if(stat != null){
                	zooKeeper.getData(path, false, getDataCallback, ctx);
                    LOG.info("Status node is there: " + path);
                } 
                
                break;
            case NONODE:
                break;
            default:     
                LOG.error("Something went wrong when " +
                		"checking if the status node exists: " + 
                        KeeperException.create(Code.get(rc), path));
                
                break;
            }
        }
    };
        
    DataCallback getDataCallback = new DataCallback(){
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Try again.
                 */
            	zooKeeper.getData(path, false, getDataCallback, ctxMap.get(path));
                return;
            case OK:
                /*
                 *  Print result
                 */
                String taskResult = new String(data);
                LOG.info("Task " + path + ", " + taskResult);
                
                /*
                 *  Setting the status of the task
                 */
                assert(ctx != null);
                ((TaskObject) ctx).setStatus(taskResult.contains("done"));
                
                /*
                 *  Delete status znode
                 */
                //zk.delete("/tasks/" + path.replace("/status/", ""), -1, taskDeleteCallback, null);
                zooKeeper.delete(path, -1, taskDeleteCallback, null);
                ctxMap.remove(path);
                break;
            case NONODE:
                LOG.warn("Status node is gone!");
                return; 
            default:
                LOG.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));               
            }
        }
    };
    
    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */
      
    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/nextlevel/es/indexer/tasks".equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
        zooKeeper.getChildren("/nextlevel/es/indexer/tasks", 
                tasksChangeWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
                List<String> toProcess;
                if(tasksCache == null) {
                    tasksCache = new WorkersCache(children);
                    
                    toProcess = children;
                } else {
                    toProcess = tasksCache.addedWorkersToCache( children );
                }
                
                if(toProcess != null){
                    assignTasks(toProcess);
                } 
                
                break;
            default:
                LOG.error("getChildren failed.",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void assignTasks(List<String> tasks) {
        for(String task : tasks){
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        zooKeeper.getData("/nextlevel/es/indexer/tasks/" + task, 
                false, 
                taskDataCallback, 
                task);
    }
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                
                break;
            case OK:
                /*
                 * Choose worker at random.
                 */
                List<String> list = workersCache.getWorkers();
                String designatedWorker = list.get(random.nextInt(list.size()));
                
                /*
                 * Assign task to randomly chosen worker.
                 */
                String assignmentPath = "/nextlevel/es/indexer/assign/" + 
                        designatedWorker + 
                        "/" + 
                        (String) ctx;
                
                createParent("/nextlevel/es/indexer/assign/" + designatedWorker, new byte[0]);
                
                LOG.info( "Assignment path: " + assignmentPath );
                System.out.println("Assignment path: " + assignmentPath );
                createAssignment(assignmentPath, data);
                
                break;
            default:
                LOG.error("Error when trying to get task data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void createAssignment(String path, byte[] data){
        zooKeeper.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                assignTaskCallback, 
                data);
    }
    
    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                
                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                deleteTask(name.substring( name.lastIndexOf("/") + 1));
                
                break;
            case NODEEXISTS: 
                LOG.warn("Task already assigned");
                
                break;
            default:
                LOG.error("Error when trying to assign task.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name){
        zooKeeper.delete("/nextlevel/es/indexer/tasks/" + name, -1, taskDeleteCallback, null);
    }
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);
                
                break;
            case OK:
                LOG.info("Successfully deleted " + path);
                
                break;
            case NONODE:
                LOG.info("Task has been deleted already");
                
                break;
            default:
                LOG.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };   
    
    
	public void bootstrapAsLeader() {
		createParent("/nextlevel/es/indexer/workers", new byte[0]);
		createParent("/nextlevel/es/indexer/tasks", new byte[0]);
		createParent("/nextlevel/es/indexer/assign", new byte[0]);
		createParent("/nextlevel/es/indexer/status", new byte[0]);
		
        LOG.info("Going for list of workers");
        getWorkers();
        
        (new RecoveredAssignments(zooKeeper)).recover( new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == RecoveryCallback.FAILED) {
                    LOG.error("Recovery of assigned tasks failed.");
                } else {
                    LOG.info( "Assigning recovered tasks" );
                    getTasks();
                }
            }
        });
	}
	
    void getWorkers(){
        zooKeeper.getChildren("/nextlevel/es/indexer/workers", 
                workersChangeWatcher, 
                workersGetChildrenCallback, 
                null);
    }
    
    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/nextlevel/es/indexer/workers".equals( e.getPath() );
                
                getWorkers();
            }
        }
    };  
    
    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("Succesfully got a list of workers: " 
                        + children.size() 
                        + " workers");
                reassignAndSet(children);
                break;
            default:
                LOG.error("getChildren failed",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };	

    /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */
    
    void reassignAndSet(List<String> children){
        List<String> toProcess;
        
        if(workersCache == null) {
            workersCache = new WorkersCache(children);
            toProcess = null;
        } else {
            LOG.info( "Removing and setting" );
            toProcess = workersCache.removedWorkersFromCache( children );
        }
        
        if(toProcess != null) {
            for(String worker : toProcess){
                getAbsentWorkerTasks(worker);
            }
        }
    }
    
    void getAbsentWorkerTasks(String worker){
        zooKeeper.getChildren("/nextlevel/es/indexer/assign/" + worker, false, workerAssignmentCallback, null);
    }
    
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                
                break;
            case OK:
                LOG.info("Succesfully got a list of assignments: " 
                        + children.size() 
                        + " tasks");
                
                /*
                 * Reassign the tasks of the absent worker.  
                 */
                
                for(String task: children) {
                    getDataReassign(path + "/" + task, task);                    
                }
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. * 
     ************************************************
     */
    
    /**
     * Get reassigned task data.
     * 
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        zooKeeper.getData(path, 
                false, 
                getDataReassignCallback, 
                task);
    }
    
    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx); 
                
                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
                
                break;
            default:
                LOG.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zooKeeper.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }
    
    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);
                
                break;
            case NODEEXISTS:
                LOG.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);
                
                break;
            default:
                LOG.error("Something wwnt wrong when recreating task", 
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
        zooKeeper.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };    
    
    void createParent(String path, byte[] data){
        zooKeeper.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                createParentCallback, 
                data);
    }
    
    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                createParent(path, (byte[]) ctx);
                
                break;
            case OK:
                LOG.info("Parent created");
                
                break;
            case NODEEXISTS:
                LOG.warn("Parent already registered: " + path);
                
                break;
            default:
                LOG.error("Something went wrong: ", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };    
    
    
    
    
    @Override
    public void close() 
            throws IOException
    {
        LOG.info( "Closing" );
        try{
            zooKeeper.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }   

	public ZooKeeper getZooKeeper() {
		return zooKeeper;
	}
    
	/**
	 * TaskObject: Holds the data for a task to be executed
	 *
	 */
    static class TaskObject {
    	private JSONObject task;
        private String taskName;
        private boolean done = false;
        private boolean succesful = false;
        private CountDownLatch latch = new CountDownLatch(1);
        
        JSONObject getTask () {
            return task;
        }
        
        void setTask (JSONObject task) {
            this.task = task;
        }
        
        void setTaskName(String name){
            this.taskName = name;
        }
        
        String getTaskName (){
            return taskName;
        }
        
        void setStatus (boolean status){
            succesful = status;
            done = true;
            latch.countDown();
        }
        
        void waitUntilDone () {
            try{
                latch.await();
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException while waiting for task to get done");
            }
        }
        
        synchronized boolean isDone(){
            return done;     
        }
        
        synchronized boolean isSuccesful(){
            return succesful;
        }    	
    }
}
