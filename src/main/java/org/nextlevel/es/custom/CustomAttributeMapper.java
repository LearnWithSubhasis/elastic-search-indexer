package org.nextlevel.es.custom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author nextlevel
 *
 */
public class CustomAttributeMapper implements ICustomMapper<CustomAttribute> {

	private String jsonString;
	private ObjectMapper objectMapper;
	private JsonGenerator jsonGenerator;

	@Override
	public void setData(JsonGenerator jsonGenerator, ObjectMapper objectMapper, String jsonString) {
		this.jsonGenerator = jsonGenerator;
		this.objectMapper = objectMapper;
		this.jsonString = jsonString;
	}

	@Override
	public List<CustomAttribute> getObject() {
		List<CustomAttribute> customAttrList = new ArrayList<CustomAttribute>();
		TypeReference<List<CustomAttribute>> typeRef = new TypeReference<List<CustomAttribute>>() {};
		try {
			jsonString = correctJsonData(jsonString);
			customAttrList = objectMapper.readValue(jsonString, typeRef);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return customAttrList;
	}

	/**
	 * When string is larger than 4000 characters, SQL Server is returning truncated data (max 4000 characters) - this is
	 * making the returned JSON invalid at times.
	 * @param jsonString2
	 * @return
	 */
	private String correctJsonData(String jsonString) {
		if (null != jsonString) {
			if (!jsonString.endsWith("\"} ]")) {
				//TODO::performance ??
				int lastCurlyBracket = jsonString.lastIndexOf("}");
				if(lastCurlyBracket > 0) {
					jsonString = jsonString.substring(0, lastCurlyBracket);
					jsonString = jsonString + "}]";
				} else {
					jsonString = jsonString + "\"}]";
				}
			}
		}
		
		return jsonString;
	}

	@Override
	public void writeObject() {
		List<CustomAttribute> object = getObject();
		try {
			jsonGenerator.writeObject(object);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
