package org.nextlevel.es.config;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author nextlevel
 *
 */
@XmlRootElement (name="IndexAll")
public class IndexConfig {
	
	private String parentIndexName;
	private ArrayList<Index> indexes;
	private int BatchCount;
	private boolean multiThreaded;
	private boolean deleteRecordsAfterIndexing;

	@XmlElement (name="Index")
	public ArrayList<Index> getIndexes() {
		return indexes;
	}

	public void setIndexes(ArrayList<Index> indexes) {
		this.indexes = indexes;
	}
	
	@XmlAttribute (name="parentIndex")
	public String getParentIndexName() {
		return parentIndexName;
	}

	public void setParentIndexName(String parentIndexName) {
		this.parentIndexName = parentIndexName;
	}

	@XmlElement (name="BatchCount")
	public int getBatchCount() {
		return BatchCount;
	}

	public void setBatchCount(int batchCount) {
		BatchCount = batchCount;
	}
	
	@XmlElement (name="MultiThreaded")
	public boolean isMultiThreaded() {
		return multiThreaded;
	}

	public void setMultiThreaded(boolean multiThreaded) {
		this.multiThreaded = multiThreaded;
	}

	@XmlElement (name="DeleteRecordsAfterIndexing")
	public boolean isDeleteRecordsAfterIndexing() {
		return deleteRecordsAfterIndexing;
	}

	public void setDeleteRecordsAfterIndexing(boolean deleteRecordsAfterIndexing) {
		this.deleteRecordsAfterIndexing = deleteRecordsAfterIndexing;
	}
}
