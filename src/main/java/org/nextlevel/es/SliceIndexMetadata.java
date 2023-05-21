package org.nextlevel.es;

import java.util.HashMap;

/**
 * 
 * @author nextlevel
 *
 */
public class SliceIndexMetadata {
	private String sliceID;
	private HashMap<String, HashMap<String, SliceIndexMetadataRecord>> mapIndexDataForSlices = new HashMap<String, HashMap<String, SliceIndexMetadataRecord>>();
	public String getSliceID() {
		return sliceID;
	}
	public void setSliceID(String sliceID) {
		this.sliceID = sliceID;
	}
	public HashMap<String, HashMap<String, SliceIndexMetadataRecord>> getMapIndexDataForSlices() {
		return mapIndexDataForSlices;
	}
	public void setMapIndexDataForSlices(HashMap<String, HashMap<String, SliceIndexMetadataRecord>> mapIndexDataForSlices) {
		this.mapIndexDataForSlices = mapIndexDataForSlices;
	}
	public HashMap<String, SliceIndexMetadataRecord> getMapIndexDataForSlice (int sliceID) {
		return this.mapIndexDataForSlices.get(sliceID);
	}
	
	/**
	 * If status = Deleted, returning last index run time as 0 (from beginning)
	 * @param sliceID
	 * @param indexName
	 * @return
	 */
	public long getLastIndexRunTime(String sliceID, String indexName) {
		long lLastIndexRunTime = 0;
		HashMap<String, SliceIndexMetadataRecord> mapIndexDataForSlice = mapIndexDataForSlices.get(sliceID);
		if(null != mapIndexDataForSlice) {
			SliceIndexMetadataRecord record = mapIndexDataForSlice.get(indexName);
			if(null != record) {				
				IndexStatus status = IndexStatus.valueOf(record.getIndexStatus());
				if(status != IndexStatus.Deleted) {			
					lLastIndexRunTime = record.getlLastIndexRunTime();
				}
			}
		}
		
		return lLastIndexRunTime;
	}
	public SliceIndexMetadataRecord getSliceIndexMetadataRecord(String sliceID, String indexName) {
		HashMap<String, SliceIndexMetadataRecord> mapIndexDataForSlice = mapIndexDataForSlices.get(sliceID);
		if(null != mapIndexDataForSlice) {
			SliceIndexMetadataRecord record = mapIndexDataForSlice.get(indexName);
			return record;
		}
		
		return null;
	}
}
