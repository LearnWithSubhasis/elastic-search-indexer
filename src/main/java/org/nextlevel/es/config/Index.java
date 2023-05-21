package org.nextlevel.es.config;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="Index")
public class Index {
	private String name;
	private String dataSource;
	private String documentType;
	private String parentIndexName;
	private String IDKey;
	private String modifiedDateKey;
	private CustomMapping customMapping;
	private boolean isCustomMappingAvailable;
	private List<IndexQuery> allQueries;
	private CreateMappingWithParent createWithParent;
	private boolean skipIndexCreation;
	
	@XmlAttribute
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@XmlElement (name="DataSource")
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	@XmlElement (name="DocumentType")
	public String getDocumentType() {
		return documentType;
	}
	public void setDocumentType(String documentType) {
		this.documentType = documentType;
	}
	@XmlElement (name="ParentIndex")
	public String getParentIndexName() {
		return parentIndexName;
	}
	public void setParentIndexName(String parentIndexName) {
		this.parentIndexName = parentIndexName;
	}
	@XmlElement (name="IDKey")
	public String getIDKey() {
		return IDKey;
	}
	public void setIDKey(String iDKey) {
		IDKey = iDKey;
	}
	@XmlElement (name="ModifiedDate")
	public String getModifiedDateKey() {
		if(null == modifiedDateKey || modifiedDateKey.trim().length()==0) {
			modifiedDateKey = "MODIFIED_DATE";
		}
		return modifiedDateKey;
	}
	public void setModifiedDateKey(String modifiedDateKey) {
		this.modifiedDateKey = modifiedDateKey;
	}
	@XmlElement (name="CustomMapping")
	public CustomMapping getCustomMapping() {
		return customMapping;
	}
	public void setCustomMapping(CustomMapping customMapping) {
		this.customMapping = customMapping;
		
		if(null != customMapping && null != customMapping.getAttributes() && customMapping.getAttributes().size()>0) {
			setCustomMappingAvailable(true);
		} else {
			setCustomMappingAvailable(false);
		}
	}
	public boolean isCustomMappingAvailable() {
		return isCustomMappingAvailable;
	}
	public void setCustomMappingAvailable(boolean isCustomMappingAvailable) {
		this.isCustomMappingAvailable = isCustomMappingAvailable;
	}
	public List<IndexQuery> getAllQueries() {
		return allQueries;
	}
	@XmlElement (name="Query")
	public void setAllQueries(List<IndexQuery> allQueries) {
		this.allQueries = allQueries;
	}

	public String getQuery(String dbType) {
		if(null != allQueries) {
			for (IndexQuery indexQuery : allQueries) {
				if(dbType.equalsIgnoreCase(indexQuery.getDbType())) {
					return indexQuery.getQuery();
				}
			}
		}
		
		return null;
	}
	
	//TODO::This is just to keep the API backward compatible
	public String getQuery() {
		if(null != allQueries && allQueries.size()>0) {
			return allQueries.get(0).getQuery();
		}
		
		return null;
	}
	
	//@XmlElement(name="CreateMappingWithParent")
	public CreateMappingWithParent getCreateWithParent() {
		return createWithParent;
	}
	public void setCreateWithParent(CreateMappingWithParent createWithParent) {
		this.createWithParent = createWithParent;
	}
	
	@XmlElement(name="SkipIndexCreation")
	public boolean isSkipIndexCreation() {
		return skipIndexCreation;
	}
	public void setSkipIndexCreation(boolean skipIndexCreation) {
		this.skipIndexCreation = skipIndexCreation;
	}
}
