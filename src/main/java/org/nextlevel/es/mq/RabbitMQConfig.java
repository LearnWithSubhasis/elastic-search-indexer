package org.nextlevel.es.mq;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="RabbitMQ")
public class RabbitMQConfig {
	private String host;
	private int port;
	private String virtualHost;
	private String user;
	private String password;
	
	@XmlElement(name="Host")
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	@XmlElement(name="Port")
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	@XmlElement(name="VirtualHost")
	public String getVirtualHost() {
		return virtualHost;
	}
	public void setVirtualHost(String virtualHost) {
		this.virtualHost = virtualHost;
	}
	@XmlElement(name="User")
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	@XmlElement(name="Password")
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}	
}
