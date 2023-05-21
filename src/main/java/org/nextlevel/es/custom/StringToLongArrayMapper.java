package org.nextlevel.es.custom;

import java.io.IOException;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author nextlevel
 *
 */
public class StringToLongArrayMapper implements ICustomMapper<Long> {

	private String jsonString;
	private ObjectMapper objectMapper;
	private JsonGenerator jsonGenerator;

	private static HashMap<String, long[]> mapConvertedData = new HashMap<String, long[]>();
	
	@Override
	public void setData(JsonGenerator jsonGenerator, ObjectMapper objectMapper, String jsonString) {
		this.jsonGenerator = jsonGenerator;
		this.objectMapper = objectMapper;
		this.jsonString = jsonString;
	}

	@Override
	public long[] getObject() {
		long[] values = null;
		if(null != jsonString) {
			if(mapConvertedData.containsKey(jsonString)) {
				return mapConvertedData.get(jsonString);
			}
			
			try {
				if(jsonString.indexOf(",") > 0) {
					String[] arr = jsonString.split(",");
					values = new long[arr.length];
					for (int i=0; i<arr.length; i++) {
						values[i] = Long.valueOf(arr[i]);
					}
				} else {
					values = new long[1];
					values[0] = Long.valueOf(jsonString);				
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			mapConvertedData.put(jsonString, values);
		}
		return values;
	}

	@Override
	public void writeObject() {
		long[] values = getObject();
		try {
			jsonGenerator.writeObject(values);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
}
