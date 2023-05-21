package org.nextlevel.es;

public enum ListenerEventType {
	CREATE ("Create"), 
	UPDATE ("Update"), 
	DELETE ("Delete"), 
	BULKUPDATE ("BulkUpdate"), 
	BULKDELETE ("BulkDelete"),
	SLICE_DEPROVISION ("SliceDeprovisioned"),
	ACCESS_CHANGED_INSTANCE ("ACCESS_CHANGED_INSTANCE"),
	ACCESS_CHANGED_OBS ("ACCESS_CHANGED_OBS"),
	ACCESS_CHANGED_GROUP ("ACCESS_CHANGED_GROUP");
	
	private String value;

	private ListenerEventType(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}		
}	
