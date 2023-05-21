package org.nextlevel.es;

/**
 * 
 * @author nextlevel
 *
 */
public class SliceIndexMetadataRecord {
	private String sliceID;
	private String documentType;
	private long lLastIndexRunTime;
	private long lIndexStartedAt;
	private String indexStatus;
	private String lastIndexType;
	private String appId;
	
	public SliceIndexMetadataRecord(String sliceID, String indexDocType,
			long lastIndxedOn, String indexStatus, String lastIndexType,
			long lIndexStartedAt, String appId) {
				this.sliceID = sliceID;
				documentType = indexDocType;
				lLastIndexRunTime = lastIndxedOn;
				this.indexStatus = indexStatus;
				this.lastIndexType = lastIndexType;
				this.lIndexStartedAt = lIndexStartedAt;
				this.setAppId(appId);
	}
	public String getSliceID() {
		return sliceID;
	}
	public void setSliceID(String sliceID) {
		this.sliceID = sliceID;
	}
	public String getDocumentType() {
		return documentType;
	}
	public void setDocumentType(String documentType) {
		this.documentType = documentType;
	}
	public long getlLastIndexRunTime() {
		return lLastIndexRunTime;
	}
	public void setlLastIndexRunTime(long lLastIndexRunTime) {
		this.lLastIndexRunTime = lLastIndexRunTime;
	}
	public long getlIndexStartedAt() {
		return lIndexStartedAt;
	}
	public void setlIndexStartedAt(long lIndexStartedAt) {
		this.lIndexStartedAt = lIndexStartedAt;
	}
	public String getIndexStatus() {
		if(null == indexStatus || indexStatus.trim().length()==0) {
			indexStatus = IndexStatus.NotRun.getValue();
		}
		return indexStatus;
	}
	public void setIndexStatus(String indexStatus) {
		this.indexStatus = indexStatus;
	}
	public String getLastIndexType() {
		return lastIndexType;
	}
	public void setLastIndexType(String lastIndexType) {
		this.lastIndexType = lastIndexType;
	}
	
	public String toString() {
		return new StringBuffer("slice=").append(sliceID).append(",status=").append(indexStatus).append(",indexType=").append(lastIndexType).append(",lastRunTime=").
				append(lLastIndexRunTime).append(",startedAt=").append(lIndexStartedAt).append(",index=").append(documentType).toString();
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
}
