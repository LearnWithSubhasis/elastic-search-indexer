package org.nextlevel.es.tc;

import java.net.InetAddress;
import java.util.ArrayList;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is based on ES 5.2.x API
 * @author nextlevel
 *
 */
public class TransportClientProvider {
	private static Logger logger = LoggerFactory.getLogger(TransportClientProvider.class);	
	private static TransportClientProvider instance = new TransportClientProvider();
	private ElasticSearchConfiguration esConfig = ElasticSearchConfigurationReader.getInstance().getConfiguration();
	private Client client = null;
	
	//Restricting instantiation - Singleton
	private TransportClientProvider() {	
		logger.info("Establishing connection with ElasticSearch...");
		System.out.println("Establishing connection with ElasticSearch...");
		
		logger.info("Cluster: " + esConfig.getClusterName());
		System.out.println("Cluster: " + esConfig.getClusterName());
		ArrayList<ServerHost> serverHosts = esConfig.getServerHosts();
		for (ServerHost host : serverHosts) {
			System.out.println("Server: " + host.getNodeName());
			logger.info("Server: " + host.getNodeName());
		}
		System.out.println("Port: " + esConfig.getTransportClientPort());
		logger.info("Port: " + esConfig.getTransportClientPort());
		
//		Settings settings = Settings.settingsBuilder() //deprecated
		Builder settingsBuilder = Settings.builder();
		
		if(esConfig.isXPackEnabled()) {
			settingsBuilder.put("cluster.name", esConfig.getClusterName());
			settingsBuilder.put("request.headers.X-Found-Cluster", esConfig.getClusterName());
			
			StringBuffer sbUserPwd = new StringBuffer(esConfig.getElasticSearchUser())
				.append(":").append(esConfig.getElasticSearchPassword());
			settingsBuilder.put("xpack.security.user", sbUserPwd.toString());
			
			if(esConfig.isTLSEnabled()) {
				settingsBuilder.put("xpack.ssl.certificate_authorities", esConfig.getTLSPathToCertificateAuth())
				.put("xpack.ssl.key", esConfig.getTLSPathToCertificateKey())
                .put("xpack.ssl.certificate", esConfig.getTLSPathToCertificateFile())
				.put("xpack.security.transport.ssl.enabled", true);
			}
		} else {
			settingsBuilder.put("cluster.name", esConfig.getClusterName());
		}
		
		Settings settings = settingsBuilder.build();

		try {
			//client = TransportClient.builder().settings(settings)
			//           .build();
			if(esConfig.isXPackEnabled()) {
				client = new PreBuiltXPackTransportClient(settings);
			} else {			
				client = new PreBuiltTransportClient(settings);
			}
			
			for (ServerHost host : serverHosts) {
				((TransportClient)client).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.getNodeName()), esConfig.getTransportClientPort()));
			}
		} catch (Throwable e) {
			e.printStackTrace();
			logger.error("Failed to establish connection with ElasticSearch: "+ e.getMessage());
		}
	}
	
	public static TransportClientProvider getInstance() {
		return instance;
	}
	
	public Client getTransportClient() throws Exception {
		if (null == client) {
			synchronized (this) {
				logger.warn("Trying to re-establish the connection to ElasticSearch...");
				new TransportClientProvider();
			}
		}
		
		if(null == client) {
			throw new Exception("ElasticSearch client was not initialized properly. Please check if the ElasticSearch server is running.");
		}
		
		return client;
	}
	
	public void close() {
		client.close();
	}
	
	public static void main(String[] args) {
		Client clientTemp = null;
		try {
			TransportClientProvider tcp = TransportClientProvider.getInstance();
			clientTemp = tcp.getTransportClient();
			ActionFuture<ClusterStateResponse> clusterStateAct = clientTemp.admin().cluster().state(new ClusterStateRequest());
			ClusterStateResponse clusterState = clusterStateAct.get();
			String clusterName = clusterState.getState().getClusterName().value();
			System.out.println("Cluster Name:: " + clusterName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(null != clientTemp) {
				clientTemp.close();
			}
		}
	}
	

}
