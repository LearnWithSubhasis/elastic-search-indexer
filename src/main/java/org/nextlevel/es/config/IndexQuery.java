package org.nextlevel.es.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="Query")
public class IndexQuery {
	private String dbType;
	private String query;
	
	public String getDbType() {
		return dbType;
	}
	@XmlAttribute (name="dbType")
	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	public String getQuery() {
		//query = (query != null) ? query.toLowerCase() : query; 
		return query;
	}
	@XmlElement (name="QueryText")
	public void setQuery(String query) {
		//query = (query != null) ? query.toLowerCase() : query; 
		this.query = query;
	}
}
