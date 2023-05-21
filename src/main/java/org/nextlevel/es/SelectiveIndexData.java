package org.nextlevel.es;

/**
 * 
 * @author nextlevel
 *
 */
public class SelectiveIndexData {
//	private long rowID;
	private String slice;
	private long itemID;
	private String documentType;
	private String operationTypeString;
	private ListenerEventType eventType;
	public SelectiveIndexData(String sliceID, long itemID2, String indexDocType, String operationTypeStr) {
//		rowID = rowID2;
		slice = sliceID;
		itemID = itemID2;
		documentType = indexDocType;
		operationTypeString = operationTypeStr;
		
		resolveEventType();
	}
	private void resolveEventType() {
		if(null != operationTypeString) {
			ListenerEventType[] eventTypes = ListenerEventType.values();
			for (ListenerEventType listenerEventType : eventTypes) {
				if(listenerEventType.getValue().equalsIgnoreCase(operationTypeString)) {
					eventType = listenerEventType;
					break;
				}
			}
		}
	}
//	public long getRowID() {
//		return rowID;
//	}
//	public void setRowID(long rowID) {
//		this.rowID = rowID;
//	}
	public String getSlice() {
		return slice;
	}
	public void setSlice(String slice) {
		this.slice = slice;
	}
	public long getItemID() {
		return itemID;
	}
	public void setItemID(long itemID) {
		this.itemID = itemID;
	}
	public String getDocumentType() {
		return documentType;
	}
	public void setDocumentType(String documentType) {
		this.documentType = documentType;
	}
	public String getOperationTypeString() {
		return operationTypeString;
	}
	public void setOperationTypeString(String operationTypeString) {
		this.operationTypeString = operationTypeString;
		resolveEventType();
	}
	public ListenerEventType getEventType() {
		return eventType;
	}
	public void setEventType(ListenerEventType eventType) {
		this.eventType = eventType;
	}
	
	@Override
	public String toString() {
		StringBuffer sbData = new StringBuffer("+++");
		sbData.append("Slice: ").append(slice).append(", Document Type: ").append(documentType)
			.append(", ID: ").append(itemID).append(", Actual Op Type: ").append(operationTypeString)
			.append(", Event Type: ").append(eventType).append(".");
		return sbData.toString();
	}

}
