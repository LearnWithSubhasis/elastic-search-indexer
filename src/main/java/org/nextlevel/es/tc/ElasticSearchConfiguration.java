package org.nextlevel.es.tc;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="ElasticSearchserverConfig")
public class ElasticSearchConfiguration {

	private String ClusterName;
	private ArrayList<ServerHost> ServerHosts;
	private int TransportClientPort;
	private int HttpPort;
	private boolean ClientTransportSniff;
	private boolean XPackEnabled;
	private String ElasticSearchUser;
	private String ElasticSearchPassword;
	private boolean TLSEnabled;
	private String TLSPathToCertificateAuth;
	private String TLSPathToCertificateKey;
	private String TLSPathToCertificateFile;
	
	@XmlElement (name="ClusterName")
	public String getClusterName() {
		return ClusterName;
	}
	public void setClusterName(String clusterName) {
		ClusterName = clusterName;
	}
	@XmlElement (name="TransportClientPort")
	public int getTransportClientPort() {
		return TransportClientPort;
	}
	public void setTransportClientPort(int transportClientPort) {
		TransportClientPort = transportClientPort;
	}
	@XmlElement (name="ClientTransportSniff")
	public boolean isClientTransportSniff() {
		return ClientTransportSniff;
	}
	public void setClientTransportSniff(boolean clientTransportSniff) {
		ClientTransportSniff = clientTransportSniff;
	}
	@XmlElement (name="HttpPort")
	public int getHttpPort() {
		return HttpPort;
	}
	public void setHttpPort(int httpPort) {
		HttpPort = httpPort;
	}
	@XmlElement (name="ServerHost")	
	public ArrayList<ServerHost> getServerHosts() {
		return ServerHosts;
	}
	public void setServerHosts(ArrayList<ServerHost> serverHosts) {
		ServerHosts = serverHosts;
	}
	@XmlElement (name="XPackEnabled")
	public boolean isXPackEnabled() {
		return XPackEnabled;
	}
	public void setXPackEnabled(boolean xPackEnabled) {
		XPackEnabled = xPackEnabled;
	}
	@XmlElement (name="ElasticSearchUser")
	public String getElasticSearchUser() {
		return ElasticSearchUser;
	}
	public void setElasticSearchUser(String elasticSearchUser) {
		ElasticSearchUser = elasticSearchUser;
	}
	@XmlElement (name="ElasticSearchPassword")
	public String getElasticSearchPassword() {
		return ElasticSearchPassword;
	}
	public void setElasticSearchPassword(String elasticSearchPassword) {
		ElasticSearchPassword = elasticSearchPassword;
	}
	@XmlElement (name="TLSEnabled")
	public boolean isTLSEnabled() {
		return TLSEnabled;
	}
	public void setTLSEnabled(boolean tLSEnabled) {
		TLSEnabled = tLSEnabled;
	}
	@XmlElement (name="TLSPathToCertificateAuth")
	public String getTLSPathToCertificateAuth() {
		return TLSPathToCertificateAuth;
	}
	public void setTLSPathToCertificateAuth(String tLSPathToCertificateAuth) {
		TLSPathToCertificateAuth = tLSPathToCertificateAuth;
	}
	@XmlElement (name="TLSPathToCertificateKey")
	public String getTLSPathToCertificateKey() {
		return TLSPathToCertificateKey;
	}
	public void setTLSPathToCertificateKey(String tLSPathToCertificateKey) {
		TLSPathToCertificateKey = tLSPathToCertificateKey;
	}
	@XmlElement (name="TLSPathToCertificateFile")
	public String getTLSPathToCertificateFile() {
		return TLSPathToCertificateFile;
	}
	public void setTLSPathToCertificateFile(String tLSPathToCertificateFile) {
		TLSPathToCertificateFile = tLSPathToCertificateFile;
	}
	
}
