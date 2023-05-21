package org.nextlevel.es;

import org.nextlevel.es.db.util.DatabaseUtil;
import org.nextlevel.es.mapping.MappingManager;

/**
 * 
 * @author nextlevel
 *
 */
public final class IndexManagerFactory {
	private static IndexManagerFactory instance = new IndexManagerFactory();
	private IndexOperationType indexOperationType;
	private IndexManager indexManager;
	private DatabaseUtil dbUtil = new DatabaseUtil();
	
	private IndexManagerFactory() {
		initializeIndexAndMappings();
		dbUtil.initializeSliceDBPools();
	}
	
	public static IndexManagerFactory getInstance() {
		return instance;
	}

	public IndexOperationType getIndexOperationType() {
		return indexOperationType;
	}

	public void setIndexOperationType(IndexOperationType indexOperationType) {
		this.indexOperationType = indexOperationType;
	}

	public IndexManager getIndexManager() {
		switch(indexOperationType) {
		case Full:
			indexManager = new FullIndexManager();			
			break;
			
		case Incremental:
			indexManager = new IncrementalIndexManager();
			break;
			
		case Selective:
			indexManager = new SelectiveIndexManager();
			break;
			
			default:
				System.err.println("The requested indexing type <" + indexOperationType + "> is not enabled !!!");
		}
		
		return indexManager;
	}
	
	protected void initializeIndexAndMappings() {
		MappingManager mm = MappingManager.getInstance();
		mm.initialize();
	}
}
