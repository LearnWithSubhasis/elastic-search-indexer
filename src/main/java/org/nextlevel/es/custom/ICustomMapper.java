package org.nextlevel.es.custom;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author nextlevel
 *
 * @param <E>
 */
public interface ICustomMapper<E> {
	void setData(JsonGenerator jsonGenerator, ObjectMapper objectMapper, String jsonString);
	Object getObject();
	void writeObject();
}
