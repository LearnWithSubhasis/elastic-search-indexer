package org.nextlevel.es;

public enum IndexOperationType {
	Full ("Full"),
	Incremental ("Incremental"),
	Selective ("Selective");
	
	private String value;

	private IndexOperationType(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
}