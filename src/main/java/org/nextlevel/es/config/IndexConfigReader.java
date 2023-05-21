package org.nextlevel.es.config;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.nextlevel.es.common.ConfigUtil;
import org.nextlevel.es.common.LogInitializer;

/**
 * 
 * @author nextlevel
 *
 */
public class IndexConfigReader {
	
	private static final String INDEX_CONFIG_XML = "index-config.xml";
	private static final String PARENT_INDEX_DEFAULT = "csm";
	public static final int DEFAULT_BATCH_COUNT = 10000;
	public static final int DEFAULT_MAX_BATCH_COUNT = 500000;
	private static IndexConfigReader instance = new IndexConfigReader();
	private IndexConfig indexConfig = null; 
	private HashMap<String, Index> indexMap = new HashMap<String, Index>();
	static {
		LogInitializer.initializeLogger();
	}
	
	private IndexConfigReader(){
		initializeConfiguration();
	}
	
	public static IndexConfigReader getInstance() {
		if(null == instance) {
			instance = new IndexConfigReader();
		}
		return instance;
	}
	
	protected IndexConfig initializeConfiguration() {
		if(null == indexConfig) {
			try {
				InputStream configFile = ConfigUtil.getInstance().getIndexerConfiguration(INDEX_CONFIG_XML);			
				JAXBContext jaxbContext = JAXBContext.newInstance(IndexConfig.class);
		
				Unmarshaller jaxbUnmarshaller;
					jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				indexConfig = (IndexConfig) jaxbUnmarshaller.unmarshal(configFile);
				
				putInMap();
			} catch (JAXBException e) {
				e.printStackTrace();
			} finally {
			}
		}
		
		return indexConfig;
	}
	
	private void putInMap() {
		if(null != indexConfig) {
			ArrayList<Index> indexes = indexConfig.getIndexes();
			for (Index index : indexes) {
				indexMap.put(index.getName(), index);
			}			
		}
	}
	
	public String getParentIndexName(Index index) {
		if(null == indexConfig || null == index) {
			return null;
		}
		
		String parentIndexName = index.getParentIndexName()!=null ? index.getParentIndexName() : indexConfig.getParentIndexName();
		if(parentIndexName != null) {
			return parentIndexName;
		}
		
		return PARENT_INDEX_DEFAULT;
	}
	
	public int getBatchCount() {
		int batchCount = DEFAULT_BATCH_COUNT;
		if(null != indexConfig) {
			batchCount = indexConfig.getBatchCount();
			if(batchCount < 1 || batchCount > DEFAULT_MAX_BATCH_COUNT) {
				batchCount = IndexConfigReader.DEFAULT_BATCH_COUNT;
			}
		}
		
		return batchCount;
	}
	
	public static void main(String[] args) {
		IndexConfigReader reader = IndexConfigReader.getInstance();
		IndexConfig config = reader.getIndexConfiguration();
		if(null != config) {
			System.out.println("Delete-Post-Indexing: " +config.isDeleteRecordsAfterIndexing());
			ArrayList<Index> indexes = config.getIndexes();
			for (Index index : indexes) {
				System.out.println("-----------------");
				System.out.println(index.getName());
				
				System.out.println(index.getDocumentType());
				System.out.println(index.getParentIndexName()!=null ? index.getParentIndexName() : config.getParentIndexName());
				
				List<IndexQuery> allQueries = index.getAllQueries();
				for (IndexQuery indexQuery : allQueries) {
					System.out.println(indexQuery.getDbType() + ": " + indexQuery.getQuery());
				}
				
				CustomMapping mapping = index.getCustomMapping();
				if(null != mapping) {
					List<Attribute> attributes = mapping.getAttributes();
					if(null != attributes) {
						for (Attribute attribute : attributes) {
							System.out.println(attribute.getAttributeID() + ", " + attribute.getMapperClass());
						}
					}
				}
			}
		}
	}
	
	public Index getIndexDetails(String indexName) {
		if(null != indexName)
			return indexMap.get(indexName);
		
		return null;
	}
	
	public HashMap<String, Index> getIndexMap() {
		return indexMap;
	}
	
	public IndexConfig getIndexConfiguration() {
		return indexConfig;
	}
}
