package org.nextlevel.es.zk.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement (name="ZooKeeper")
public class ZooKeeperConfig {
	private int nodeID;
	private String zkHostName;
	private int zkPortNumber = -1;
	private String processNodePrefix;
	private String pathForElection;
	private boolean masterCanWork;
	
	@XmlElement(name="NodeID")
	public int getNodeID() {
		return nodeID;
	}
	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}
	@XmlElement(name="Host")
	public String getZkHostName() {
		return zkHostName;
	}
	public void setZkHostName(String zkHostName) {
		this.zkHostName = zkHostName;
	}
	@XmlElement(name="Port")
	public int getZkPortNumber() {
		return zkPortNumber;
	}
	public void setZkPortNumber(int zkPortNumber) {
		this.zkPortNumber = zkPortNumber;
	}
	@XmlElement(name="ProcessNodePrefix")
	public String getProcessNodePrefix() {
		return processNodePrefix;
	}
	public void setProcessNodePrefix(String processNodePrefix) {
		this.processNodePrefix = processNodePrefix;
	}
	@XmlElement(name="PathElection")
	public String getPathForElection() {
		return pathForElection;
	}
	public void setPathForElection(String pathForElection) {
		this.pathForElection = pathForElection;
	}
	
	public String getZooKeeperURL() {
		if(zkPortNumber > 0 && zkPortNumber < 65536) {
			return new StringBuffer(zkHostName).append(":").append(zkPortNumber).toString();
		}
		
		return zkHostName;
	}
	public boolean isMasterCanWork() {
		return masterCanWork;
	}
	@XmlElement(name="MasterCanWork")
	public void setMasterCanWork(boolean masterCanWork) {
		this.masterCanWork = masterCanWork;
	}
}
