package org.nextlevel.es.tc;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="ServerHost")
public class ServerHost {
	private String NodeType;
	private String NodeName;
	public String getNodeType() {
		return NodeType;
	}
	@XmlElement (name="NodeType")
	public void setNodeType(String nodeType) {
		this.NodeType = nodeType;
	}
	public String getNodeName() {
		return NodeName;
	}
	@XmlElement (name="NodeName")
	public void setNodeName(String nodeName) {
		this.NodeName = nodeName;
	}	
}
