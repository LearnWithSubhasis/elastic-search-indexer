//package org.nextlevel.es.tc;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.concurrent.TimeUnit;
//
//import org.elasticsearch.action.ActionFuture;
//import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
//
//public class XPackTransportClientTester {
//	private static ElasticSearchConfiguration esConfig = ElasticSearchConfigurationReader
//			.getInstance().getConfiguration();
//	private static TransportClient client = null;
//
//	public static void main(String[] args) {
//
//		Settings settings = Settings
//				.builder()
//				.put("client.transport.nodes_sampler_interval", "5s")
//				.put("client.transport.sniff", false)
//				.put("transport.tcp.compress", true)
//				.put("cluster.name", esConfig.getClusterName())
//				.put("xpack.security.transport.ssl.enabled", true)
//				.put("request.headers.X-Found-Cluster",
//						esConfig.getClusterName())
//				.put("xpack.security.user", "elastic:changeme")
//				.put("xpack.ssl.certificate_authorities", esConfig.getTLSPathToCertificateAuth())
//				.put("xpack.ssl.key", esConfig.getTLSPathToCertificateKey())
//                .put("xpack.ssl.certificate", esConfig.getTLSPathToCertificateFile())
//				.build();
//
//		// Instantiate a TransportClient and add the cluster to the list of
//		// addresses to connect to.
//		// Only port 9343 (SSL-encrypted) is currently supported. The use of
//		// X-Pack security features (formerly Shield) is required.
//		client = new PreBuiltXPackTransportClient(settings);
//		try {
//			ArrayList<ServerHost> serverHosts = esConfig.getServerHosts();
//			for (ServerHost host : serverHosts) {
//				System.out.println("Server: " + host.getNodeName());
//			}
//
//			for (ServerHost host : serverHosts) {
//				((TransportClient) client)
//						.addTransportAddress(new InetSocketTransportAddress(
//								InetAddress.getByName(host.getNodeName()),
//								esConfig.getTransportClientPort()));
//			}
//		} catch (UnknownHostException e) {
//			System.err.println("Unable to get the host: " + e.getMessage());
//		}
//
//		while (true) {
//			try {
//				System.out.println("Getting cluster health... ");
//				ActionFuture<ClusterHealthResponse> healthFuture = client
//						.admin().cluster()
//						.health(Requests.clusterHealthRequest());
//				ClusterHealthResponse healthResponse = healthFuture.get(5,
//						TimeUnit.SECONDS);
//				System.out.println("Got cluster health response: "
//						+ healthResponse.getStatus());
//			} catch (Throwable t) {
//				System.err.println("Unable to get cluster health response: "
//						+ t.getMessage());
//			}
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException ie) {
//				ie.printStackTrace();
//			}
//		}
//	}
//}
//
//
