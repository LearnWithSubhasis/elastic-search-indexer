package org.nextlevel.es.zk;

import java.util.ArrayList;
import java.util.List;

public class WorkersCache {
	private List<String> workers = null;
	
	public WorkersCache() {
		this.workers = null;
	}
	
	public WorkersCache(List<String> workers) {
		this.workers = workers;
	}
	
	public List<String> getWorkers() {
		return workers;
	}
	
    List<String> addedWorkersToCache( List<String> newWorkers) {
        ArrayList<String> diff = null;
        
        if(workers == null) {
            diff = new ArrayList<String>(newWorkers);
        } else {
            for(String worker: newWorkers) {
                if(!workers.contains( worker )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                
                    diff.add(worker);
                }
            }
        }
        this.workers = newWorkers;
            
        return diff;
    }
        
    List<String> removedWorkersFromCache( List<String> newWorkers) {
        List<String> diff = null;
            
        if(workers != null) {
            for(String worker: workers) {
                if(!newWorkers.contains( worker )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                    
                    diff.add(worker);
                }
            }
        }
        this.workers = newWorkers;
        
        return diff;
    }
}
