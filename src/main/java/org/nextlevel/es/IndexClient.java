package org.nextlevel.es;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.common.ConfigUtil;
import org.nextlevel.es.common.LogInitializer;
import org.nextlevel.es.zk.ProcessNode;
import org.nextlevel.es.zk.config.ZooKeeperConfig;
import org.nextlevel.es.zk.config.ZooKeeperConfigReader;

/**
 * 
 * @author nextlevel
 *
 */
public class IndexClient {
	
	private static Logger logger = LoggerFactory.getLogger(IndexClient.class);
	static {
		LogInitializer.initializeLogger();
	}
	
	public static void main(String args[]) throws SQLException {
//		testNearRealtimeIndexing();
		
//		IndexManagerFactory imf = IndexManagerFactory.getInstance();
//		imf.setIndexOperationType(IndexOperationType.Selective);
//		IndexManager im = imf.getIndexManager();
//		im.setTenantID("tenant1");
////		im.setSelectedIDsToIndex(null);
//		im.indexNearRealTime();

//		ESIndexMetadataDetails details = new ESIndexMetadataDetails();
//		List<ESIndexMetadata> all= details.getIndexDetails(-1, "any", "Completed");
//		System.out.println(all.size());		

//		long startedAt = System.currentTimeMillis();
//		Date dt = new Date(startedAt);
//		System.out.println(dt);
//		IndexManagerFactory imf = IndexManagerFactory.getInstance();
//		imf.setIndexOperationType(IndexOperationType.Full);
//		IndexManager im = imf.getIndexManager();
//		im.setTenantID("tenant1");
//		//im.setIndexDocumentType("student");
//		im.index();
//
//		long endedAt = System.currentTimeMillis();
//		dt = new Date(endedAt);
//		System.out.println(dt);
//		System.out.println("Time taken (mins): " + ((endedAt-startedAt)/(1000*60)));
		
//		IndexManagerFactory imf = IndexManagerFactory.getInstance();
//		imf.setIndexOperationType(IndexOperationType.Full);
//		IndexManager im = imf.getIndexManager();	
//		im.setSliceID(1000000);
//		im.setIndexDocumentType("student,teacher,user");
//		im.index();
		
//		IndexManagerFactory imf = IndexManagerFactory.getInstance();
//		imf.setIndexOperationType(IndexOperationType.Full);
//		IndexManager im = imf.getIndexManager();
//		IndexConfig configData = IndexConfigReader.getInstance().getIndexConfiguration();
//		im.deleteIndexBySlice(configData.getParentIndexName(), "student, teacher", 1000000, true);
		
		parseArgumentsAndIndex(args);
	}
	
	private static void parseArgumentsAndIndex(String args[]) {
		if(args.length == 0) {
			showUsage();
			return;
		}
		
		String operationType = "full";
		String configDir = null;
		String strAsService = null;
		boolean runAsService = false;
		int i=0;
		try {
			for (String arg : args) {
				if(arg.equalsIgnoreCase("-op")) {
					operationType = args[i+1];
				} else if(arg.equalsIgnoreCase("-c")) {
					configDir = args[i+1];
				} else if(arg.equalsIgnoreCase("-service")) {
					strAsService = args[i+1];
				} else if(arg.startsWith("-") && ((!arg.equalsIgnoreCase("-op")) || (!arg.equalsIgnoreCase("-c")))) {
					throw new Exception("Invalid option provided: " + arg);
				}
				
				i++;
			}
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			showUsage();
			return;
		}	
		
		IndexOperationType opType = null;
		if(operationType.equalsIgnoreCase("incremental")) {
			opType = IndexOperationType.Incremental;
		} else if(operationType.equalsIgnoreCase("full")) {
			opType = IndexOperationType.Full;
		} else {
			System.err.println("Invalid value for operation type (-op) was provided: <" + operationType + ">.");
			showUsage();
			return;
		}
		
		if(null != configDir && configDir.trim().length()>0) {
			File configDirObj = new File(configDir);
			if(!configDirObj.isDirectory() || !configDirObj.exists()) {
				String msg = "The config directory was provided, but it does not exist: " + configDir;
				System.err.println(msg);
				
				msg = "WARNING: The configuration files will be read from ELASTIC_SEARCH_UTIL_HOME or CLASSPATH. It is suggested to provide valid path to config directory though.";
				System.out.println(msg);
				logger.warn(msg);
			}
		} else {
			String msg = "The config directory was not provided, or it does not exist.";
			System.err.println(msg);
			
			msg = "WARNING: The configuration files will be read from ELASTIC_SEARCH_UTIL_HOME or CLASSPATH. It is suggested to provide valid path to config directory though.";
			System.out.println(msg);
			logger.warn(msg);
		}
		
		if(null != strAsService && strAsService.trim().length()>0) {
			runAsService = Boolean.parseBoolean(strAsService);
		}
		
		if(!runAsService) {
			IndexManagerFactory indexManagerFactory = IndexManagerFactory.getInstance();
			indexManagerFactory.setIndexOperationType(opType);
			IndexManager indexManager = indexManagerFactory.getIndexManager();
			if(null != configDir) {
				indexManager.setConfigDir(configDir);
			}
			ConfigUtil.getInstance().setConfigPathFromArg(configDir);
			
			indexManager.index();	
		} else {
			try {
				runIndexerAsService();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("Failed to run Indexer as service: "+ e.getMessage());
			}
		}
	}

	private static void runIndexerAsService() throws IOException {
		final ExecutorService service = Executors.newSingleThreadExecutor();
		final ZooKeeperConfig zkConfig = ZooKeeperConfigReader.getInstance().getZookeeperConfiguration();
		
		final Future<?> status = service.submit(new ProcessNode(zkConfig.getNodeID(), zkConfig.getZooKeeperURL()));
		
		try {
			status.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error(e.getMessage(), e);
			service.shutdown();
		}		
	}

	private static void showUsage() {
		System.err.println("Usage: java IndexClient [-op {full|incremental}]");
		System.err.println("-- OR --");
		System.err.println("Usage: java IndexClient [-op {full|incremental}] [-c <path-to-config-directory>]");
		System.err.println("-- OR --");
		System.err.println("Usage: java IndexClient [-op {full|incremental}] [-c <path-to-config-directory>] [-service <true|false>]");
	}

	private void testIndexing() {
		IndexManagerFactory indexManagerFactory = IndexManagerFactory.getInstance();
		indexManagerFactory.setIndexOperationType(IndexOperationType.Full);
		
		IndexManager indexManager = indexManagerFactory.getIndexManager();
		indexManager.setIndexDocumentType("conversations");
		//indexManager.setTenantID(999999);
		indexManager.setTenantID("tenant1");
		
		ArrayList<Long> idsToIndex = new ArrayList<Long>();
		idsToIndex.add(1L);
		idsToIndex.add(2L);
		idsToIndex.add(100L);
		
		indexManager.setSelectedIDsToIndex(idsToIndex);
		indexManager.index();
		
		boolean bExists = indexManager.isExists("999999");
		System.out.println("Exists: " + bExists);
		
		//boolean deleted = indexManager.deleteIndex("999999");
		//System.out.println("Deleted: " + deleted);		
	}
	
	private static void testNearRealtimeIndexing() {
		IndexManagerFactory indexManagerFactory = IndexManagerFactory.getInstance();
		indexManagerFactory.setIndexOperationType(IndexOperationType.Selective);
		
		IndexManager indexManager = indexManagerFactory.getIndexManager();
		indexManager.setIndexDocumentType("student");
		indexManager.setTenantID("tenant1");
		
		ArrayList<Long> idsToIndex = new ArrayList<Long>();
		idsToIndex.add(5013000L);
		
		indexManager.setSelectedIDsToIndex(idsToIndex);
		indexManager.indexNearRealTime();
		
		boolean bExists = indexManager.isExists("tenant1");
		System.out.println("Exists: " + bExists);
		
		//boolean deleted = indexManager.deleteIndex("999999");
		//System.out.println("Deleted: " + deleted);		
	}	
}
