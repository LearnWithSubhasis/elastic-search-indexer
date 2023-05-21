package org.nextlevel.es.mq;

public interface IMessageBus {
	String getHost();
	int getPort();
	String getVirtualHost();
	String getUser();
	String getPassword();
}
