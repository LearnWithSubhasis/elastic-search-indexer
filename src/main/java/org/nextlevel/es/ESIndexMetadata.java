package org.nextlevel.es;

/**
 * 
 * @author nextlevel
 *
 */
public class ESIndexMetadata {
	private int slice;
	private String sliceDB;
	private String indexDocumentType;
	private long lastIndexedOn;
	private boolean isSliceConfigEnabled;
	private String lastIndexStatus;
	private long lastIndexStartedAt;
	private String lastIndexOperationType;
	
	public int getSlice() {
		return slice;
	}
	public void setSlice(int slice) {
		this.slice = slice;
	}
	public String getIndexDocumentType() {
		return indexDocumentType;
	}
	public void setIndexDocumentType(String indexDocumentType) {
		this.indexDocumentType = indexDocumentType;
	}
	public long getLastIndexedOn() {
		return lastIndexedOn;
	}
	public void setLastIndexedOn(long lastIndexedOn) {
		this.lastIndexedOn = lastIndexedOn;
	}
	public boolean isSliceConfigEnabled() {
		return isSliceConfigEnabled;
	}
	public void setSliceConfigEnabled(boolean isSliceConfigEnabled) {
		this.isSliceConfigEnabled = isSliceConfigEnabled;
	}
	public String getSliceDB() {
		return sliceDB;
	}
	public void setSliceDB(String sliceDB) {
		this.sliceDB = sliceDB;
	}
	public String getLastIndexStatus() {
		lastIndexStatus = (lastIndexStatus == null) ? IndexStatus.NotRun.getValue() : lastIndexStatus;
		return lastIndexStatus;
	}
	public void setLastIndexStatus(String lastIndexStatus) {
		this.lastIndexStatus = lastIndexStatus;
	}
	public long getLastIndexStartedAt() {
		return lastIndexStartedAt;
	}
	public void setLastIndexStartedAt(long lastIndexStartedAt) {
		this.lastIndexStartedAt = lastIndexStartedAt;
	}
	public String getLastIndexOperationType() {
		return lastIndexOperationType;
	}
	public void setLastIndexOperationType(String lastIndexOperationType) {
		this.lastIndexOperationType = lastIndexOperationType;
	}	
}
