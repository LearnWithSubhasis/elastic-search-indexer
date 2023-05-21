package org.nextlevel.es.config;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="CustomMapping")
public class CustomMapping {
	private List<Attribute> attributes;

	@XmlElement (name="Attribute")
	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}
}
