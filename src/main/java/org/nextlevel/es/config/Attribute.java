package org.nextlevel.es.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="Attribute")
public class Attribute {
	private String attributeID;
	private String attributeType;
	private String mapperClass;

	@XmlValue
	public String getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(String mapperClass) {
		this.mapperClass = mapperClass;
	}
	
	@XmlAttribute (name="id")
	public String getAttributeID() {
		return attributeID;
	}
	public void setAttributeID(String attributeID) {
		this.attributeID = attributeID;
	}
	
	@XmlAttribute (name="type")
	public String getAttributeType() {
		return attributeType;
	}
	public void setAttributeType(String attributeType) {
		this.attributeType = attributeType;
	}
}
