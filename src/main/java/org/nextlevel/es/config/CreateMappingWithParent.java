package org.nextlevel.es.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement (name="CreateMappingWithParent")
public class CreateMappingWithParent {
	private boolean createWithParentMapping;
	private String combinedMappingFileName;
	
	@XmlValue
	public boolean isCreateWithParentMapping() {
		return createWithParentMapping;
	}
	public void setCreateWithParentMapping(boolean createWithParentMapping) {
		this.createWithParentMapping = createWithParentMapping;
	}
	
	@XmlAttribute(name="file")
	public String getCombinedMappingFileName() {
		return combinedMappingFileName;
	}
	public void setCombinedMappingFileName(String combinedMappingFileName) {
		this.combinedMappingFileName = combinedMappingFileName;
	}

}
