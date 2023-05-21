package org.nextlevel.es.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author nextlevel
 *
 */
public class ConfigUtil {
	private static Logger logger = LoggerFactory.getLogger(ConfigUtil.class);	
	private static ConfigUtil instance = new ConfigUtil();
	private static String configPathFromArg = null;
	private static String configPathFromEnvVar = null;
	
	private ConfigUtil() {}
	
	public static ConfigUtil getInstance() {
		return instance;
	}
	
	public String getConfigPath() {
		String finalConfigDir = (null != configPathFromArg) ? configPathFromArg : getConfigPathFromEnvironmentVariable(); 
		//logger.info("Config Path: " + finalConfigDir);
		return finalConfigDir;
	}
	
	public void setConfigPathFromArg(String configDir) {
		configPathFromArg = configDir;
	}
	
	private String getConfigPathFromEnvironmentVariable() {
		if(null == configPathFromEnvVar) {
			String ELASTIC_SEARCH_UTIL_HOME = System.getenv("ELASTIC_SEARCH_UTIL_HOME");
			String msg;
			if(null != ELASTIC_SEARCH_UTIL_HOME) {
				msg = "ELASTIC_SEARCH_UTIL_HOME variable is set to: " + ELASTIC_SEARCH_UTIL_HOME;
				logger.info(msg);
				System.out.println(msg);
				configPathFromEnvVar = ELASTIC_SEARCH_UTIL_HOME + getPathSeparator() + "config";
			} else {
				msg = "ELASTIC_SEARCH_UTIL_HOME variable is NOT set. This may load some configuration files from CLASSPATH, and system may not function properly. "
						+ "It is suggested to set the environment variable ELASTIC_SEARCH_UTIL_HOME.";
				logger.warn(msg);
				System.out.println(msg);
			}
		}
		return configPathFromEnvVar;
	}
	
	private String getPathSeparator() {
		return System.getProperty("file.separator");
	}
	
	private InputStream getInputStreamForConfigurationFile(String configFile, String relativeDir) {
		InputStream istream = null;
		String configDir = getConfigPath();
		if(null != configDir && null != configFile) {
			String configFileFullPath = configDir + getPathSeparator() + relativeDir + getPathSeparator() + configFile;
			try {
				//istream = IOUtils.toInputStream(configFileFullPath, "UTF-8");
				istream = new FileInputStream(configFileFullPath);				
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		} else if (null != configFile && null == istream) {
			ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
			istream = contextClassLoader.getResourceAsStream(configFile);				
		}
		
		return istream;
	}
	
	public InputStream getPlatformDBConfiguration(String platformDBFileName) {
		return getInputStreamForConfigurationFile(platformDBFileName, "db");
	}
	
	public InputStream getElasticSearchConfiguration(String elasticSearchConfigFileName) {
		return getInputStreamForConfigurationFile(elasticSearchConfigFileName, "elasticsearch");
	}	
	
	public InputStream getTenantsConfiguration(String elasticSearchConfigFileName) {
		return getInputStreamForConfigurationFile(elasticSearchConfigFileName, "tenants");
	}	

	public InputStream getIndexerConfiguration(String indexerConfigFileName) {
		return getInputStreamForConfigurationFile(indexerConfigFileName, "indexer");
	}

	public InputStream getZooKeeperConfiguration(String zookeeperConfigFileName) {
		return getInputStreamForConfigurationFile(zookeeperConfigFileName, "zookeeper");
	}

	public InputStream getMQConfiguration(String mqConfigXml) {
		return getInputStreamForConfigurationFile(mqConfigXml, "messagebus");
	}
	
	public String getLoggerConfiguration(String loggerConfigXml) {
		String configDir = getConfigPath();
		if(null != configDir && null != loggerConfigXml) {
			return configDir + getPathSeparator() + loggerConfigXml;
		}		
		
		return loggerConfigXml;
	}
}
