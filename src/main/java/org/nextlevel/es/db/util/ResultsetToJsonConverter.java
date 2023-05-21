package org.nextlevel.es.db.util;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.nextlevel.es.config.Attribute;
import org.nextlevel.es.config.CustomMapping;
import org.nextlevel.es.config.Index;
import org.nextlevel.es.custom.ICustomMapper;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 
 * @author nextlevel
 * 
 */
public class ResultsetToJsonConverter {
	private static Logger logger = LoggerFactory.getLogger(ResultsetToJsonConverter.class);
	
	private static ResultsetToJsonConverter instance = new ResultsetToJsonConverter();
	private ResultsetToJsonConverter(){}
	
	public static ResultsetToJsonConverter getInstance() {
		return instance;
	}
	
	public String convert (ResultSet resultset, Index index) throws JsonGenerationException, JsonMappingException, IOException {
		if(null == resultset)
			return null;
		
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
		objectMapper.configure(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
		objectMapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
		objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		objectMapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

		SimpleModule moduleToRegister = new SimpleModule();
		try {
			moduleToRegister.addSerializer(new CustomResultSetSerializer(objectMapper, index));
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

		objectMapper.registerModule(moduleToRegister);
		//objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		
		
		ObjectNode objectNode = objectMapper.createObjectNode();
		objectNode.putPOJO("results", resultset);

//NOTE:: please don't uncomment this, as it will navigate thru the resultset already, so transformed string will be empty :(		
//		if(logger.isDebugEnabled()) {
//			dumpResultset(resultset);
//		}
		
		StringWriter outputWriter = new StringWriter();
		objectMapper.writeValue(outputWriter, objectNode);
//		try {
//			objectMapper.writeValue(outputWriter, objectNode);
//		} catch (JsonGenerationException e) {
//			e.printStackTrace();
//		} catch (JsonMappingException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}	
		
		return outputWriter.toString();
	}

	private void dumpResultset(ResultSet resultset) {
		//System.out.println("============================================================");
		logger.debug("===============================================================");
		try {
			ResultSetMetaData metadata = resultset.getMetaData();
			int colCount = metadata.getColumnCount();
			while(resultset.next()) {
				StringBuffer sbMsg = new StringBuffer();
				for(int i=0; i<colCount; i++) {
					sbMsg.append("||").append(metadata.getColumnName(i+1)).append("::");
					int colType = metadata.getColumnType(i+1);
					switch(colType) {
					case Types.CHAR:
					case Types.VARCHAR:
						sbMsg.append(resultset.getString(i+1));
						break;
						
					case Types.INTEGER:
						sbMsg.append(resultset.getInt(i+1));
						break;
						
					case Types.BIGINT:
						sbMsg.append(resultset.getLong(i+1));
						break;
						
					case Types.DATE:
						sbMsg.append(resultset.getDate(i+1));
						break;						
					}
				}

				//System.out.println(sbMsg);
				logger.debug(sbMsg.toString());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class CustomResultSetSerializer extends JsonSerializer<ResultSet> {

	private ObjectMapper objectMapper;
	private Index index;
	private Map<String, ICustomMapper<Object>> mapCustomMappingAttributes = new HashMap<String, ICustomMapper<Object>>();

	public static class ResultSetSerializerException extends JsonProcessingException{

		private static final long serialVersionUID = -5876145416391098724L;

		public ResultSetSerializerException(Throwable cause){
            super(cause);
        }
    }
	
    public CustomResultSetSerializer(ObjectMapper objectMapper, Index index) throws ClassNotFoundException {
		this.objectMapper = objectMapper;
		this.index = index;
		resolveCustomMappingAttributes(index);
	}

	private void resolveCustomMappingAttributes(Index index2) throws ClassNotFoundException {
		CustomMapping mapping = index.getCustomMapping();
		if(null != mapping) {
			List<Attribute> customMappingAttributes = mapping.getAttributes();
			if(null != customMappingAttributes) {
				for (Attribute attribute : customMappingAttributes) {
					Class<ICustomMapper<Object>> classTemp =  (Class<ICustomMapper<Object>>) Class.forName(attribute.getMapperClass());
					ICustomMapper<Object> mapperInstance;
					try {
						if(null != classTemp) {
							mapperInstance = classTemp.getDeclaredConstructor().newInstance();
							if(null != mapperInstance) {
								mapCustomMappingAttributes.put(attribute.getAttributeID().toLowerCase(), mapperInstance);
							}
						}
					} catch (InstantiationException | IllegalAccessException
							| IllegalArgumentException | InvocationTargetException
							| NoSuchMethodException | SecurityException e) {
						e.printStackTrace();
					}
				}
			}
		}		
	}

	@Override
    public void serialize(ResultSet resultSet, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {

        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int numColumns = rsmd.getColumnCount();
            String[] columnNames = new String[numColumns];
            int[] columnTypes = new int[numColumns];

            for (int i = 0; i < columnNames.length; i++) {
                columnNames[i] = rsmd.getColumnLabel(i + 1);
                columnTypes[i] = rsmd.getColumnType(i + 1);
            }

            jsonGenerator.writeStartArray();

            while (resultSet.next()) {

                boolean b;
                long l;
                double d;

                jsonGenerator.writeStartObject();

                for (int i = 0; i < columnNames.length; i++) {

                    jsonGenerator.writeFieldName(columnNames[i].toLowerCase());
                    
                    switch (columnTypes[i]) {

                    case Types.INTEGER:
                        l = resultSet.getInt(i + 1);
                        if (resultSet.wasNull()) {
                            jsonGenerator.writeNull();
                        } else {
                            jsonGenerator.writeNumber(l);
                        }
                        break;

                    case Types.BIGINT:
                        l = resultSet.getLong(i + 1);
                        if (resultSet.wasNull()) {
                            jsonGenerator.writeNull();
                        } else {
                            jsonGenerator.writeNumber(l);
                        }
                        break;

                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        jsonGenerator.writeNumber(resultSet.getBigDecimal(i + 1));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        d = resultSet.getDouble(i + 1);
                        if (resultSet.wasNull()) {
                            jsonGenerator.writeNull();
                        } else {
                            jsonGenerator.writeNumber(d);
                        }
                        break;

                    case Types.NVARCHAR:
                    case Types.VARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.LONGVARCHAR:
                        //Apply custom mapping, if available
                    	String jsonString = resultSet.getString(i + 1);
                    	if(null == jsonString) {
                    		jsonGenerator.writeString(jsonString);
                    	} else {
                    		//jsonString = StringEscapeUtils.escapeJava(jsonString);
                    		//jsonString = jsonString.replaceAll("\"", "");
                    		//jsonString = jsonString.replaceAll("\'", "");
                    		//jsonString = escapeString(jsonString);
                    		
	                    	boolean bCustomMappingDone = false;
	                        if(index.isCustomMappingAvailable()) {
	    	                    bCustomMappingDone = applyCustomMapping(columnNames[i], jsonGenerator, jsonString);
	                        }
	                        
		                    if(bCustomMappingDone) {
		                    	continue;
	                    	} else {                    	
	                    		jsonGenerator.writeString(jsonString);
	                    	}
                    	}
                        break;

                    case Types.BOOLEAN:
                    case Types.BIT:
                        b = resultSet.getBoolean(i + 1);
                        if (resultSet.wasNull()) {
                            jsonGenerator.writeNull();
                        } else {
                            jsonGenerator.writeBoolean(b);
                        }
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        jsonGenerator.writeBinary(resultSet.getBytes(i + 1));
                        break;

                    case Types.TINYINT:
                    case Types.SMALLINT:
                        l = resultSet.getShort(i + 1);
                        if (resultSet.wasNull()) {
                            jsonGenerator.writeNull();
                        } else {
                            jsonGenerator.writeNumber(l);
                        }
                        break;

                    case Types.DATE:
                        provider.defaultSerializeDateValue(resultSet.getDate(i + 1), jsonGenerator);
                        break;

                    case Types.TIMESTAMP:
                        provider.defaultSerializeDateValue(resultSet.getTime(i + 1), jsonGenerator);
                        break;

                    case Types.BLOB:
                        Blob blob = resultSet.getBlob(i);
                        provider.defaultSerializeValue(blob.getBinaryStream(), jsonGenerator);
                        blob.free();
                        break;

                    case Types.CLOB:
                        Clob clob = resultSet.getClob(i);
                        provider.defaultSerializeValue(clob.getCharacterStream(), jsonGenerator);
                        clob.free();
                        break;

                    case Types.ARRAY:
                        throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type ARRAY");

                    case Types.STRUCT:
                        throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type STRUCT");

                    case Types.DISTINCT:
                        throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type DISTINCT");

                    case Types.REF:
                        throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type REF");

                    case Types.JAVA_OBJECT:
                    default:
                        provider.defaultSerializeValue(resultSet.getObject(i + 1), jsonGenerator);
                        break;
                    }
                }

                jsonGenerator.writeEndObject();
            }

            jsonGenerator.writeEndArray();

        } catch (SQLException e) {
            throw new ResultSetSerializerException(e);
        }
    }
	
	private String escapeString(String jsonString) {
		if(jsonString.startsWith("\"") && jsonString.endsWith("\"")) {
			String jsonString2 = jsonString.substring(1, jsonString.length()-1);
			jsonString = StringEscapeUtils.escapeJava(jsonString2);
		} else {
			jsonString = StringEscapeUtils.escapeJava(jsonString);
		}
		
		return jsonString;
	}

	private boolean applyCustomMapping(String columnName, JsonGenerator jsonGenerator, String jsonString) {
		if(mapCustomMappingAttributes.containsKey(columnName.toLowerCase())) {
			if(null == jsonString) {
				return false;
			} else { 
				ICustomMapper<Object> mapperInstance = mapCustomMappingAttributes.get(columnName.toLowerCase()); 
				if(null != mapperInstance) {
					mapperInstance.setData(jsonGenerator, objectMapper, jsonString);
					mapperInstance.writeObject();
					return true;
				}
			}
		}
		
		return false;
	}

	@Override
    public Class<ResultSet> handledType() {
        return ResultSet.class;
    }	
}
