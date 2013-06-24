package com.netflix.astyanax.recipes.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.map.module.SimpleModule;

import com.netflix.astyanax.serializers.ByteBufferSerializer;

public class MessageQueueUtils {
    private static final ObjectMapper mapper ;

    static {
        mapper = new ObjectMapper();
        
        mapper.setSerializationInclusion(Inclusion.NON_NULL);
        mapper.setSerializationInclusion(Inclusion.NON_EMPTY);
        
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.enableDefaultTyping();
        
        mapper.setVisibilityChecker(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(Visibility.ANY)
                .withGetterVisibility(Visibility.NONE)
                .withSetterVisibility(Visibility.NONE)
                .withCreatorVisibility(Visibility.NONE)
                .withIsGetterVisibility(Visibility.NONE));
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
    
    public static  <T> T deserializeByteBuffer(ByteBuffer data, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(ByteBufferSerializer.get().toBytes(data)),
                clazz);
    }

    @SuppressWarnings({ "unchecked" })
    public static  <T> T deserializeString(String data, String className) throws JsonParseException, JsonMappingException, IOException, ClassNotFoundException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()),
                Class.forName(className));
    }


}
