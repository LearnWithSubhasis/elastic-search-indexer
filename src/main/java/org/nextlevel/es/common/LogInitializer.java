package org.nextlevel.es.common;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author nextlevel
 *
 */
public class LogInitializer {
	private static Logger logger = LoggerFactory.getLogger(LogInitializer.class);		
	private static final String LOGGER_CONFIG_XML = "log4j-elasticsearch-indexer.xml";	
	private static LogInitializer instance = new LogInitializer();
	private LogInitializer(){
		String loggerConfigFile = ConfigUtil.getInstance().getLoggerConfiguration(LOGGER_CONFIG_XML);			
		DOMConfigurator.configureAndWatch(loggerConfigFile, 10000);
		logger.info("==============================================================================");
		logger.info("Config Dir: " + ConfigUtil.getInstance().getConfigPath());
		logger.info("==============================================================================");
	}
	
	public static void initializeLogger() {
	}	
}
