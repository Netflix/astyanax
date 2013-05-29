package com.netflix.astyanax.recipes.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class MessageQueueUtils {
    static final ObjectMapper mapper = new ObjectMapper();

    {
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.enableDefaultTyping();
    }

    public static <T> String serializeToString(T object) throws JsonGenerationException, JsonMappingException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mapper.writeValue(baos, object);
        baos.flush();
        return baos.toString();
    }

    public static  <T> T deserializeString(String data, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()),
                clazz);
    }

    @SuppressWarnings({ "unused", "unchecked" })
    public static  <T> T deserializeString(String data, String className) throws JsonParseException, JsonMappingException, IOException, ClassNotFoundException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()),
                Class.forName(className));
    }


}
