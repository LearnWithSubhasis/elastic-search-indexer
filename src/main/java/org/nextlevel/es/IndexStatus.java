package org.nextlevel.es;


public enum IndexStatus {
	NotRun ("Not Run"),
	Scheduled ("Scheduled"),
	Running ("Running"),
	Completed ("Completed"),
	Failed ("Failed"),
	Deleted ("Deleted");
	
	private final String fieldDescription;

    private IndexStatus(String value) {
        fieldDescription = value;
    }

    public String getValue() {
        return fieldDescription;
    }
}