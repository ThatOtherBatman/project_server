package com.teza.common.tardis;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.teza.common.util.TypeConverterUtils;

import java.io.IOException;

/**
 * User: tom
 * Date: 2/7/17
 * Time: 8:31 PM
 */
@SuppressWarnings("DuplicateThrows")
public class DocHierarchySerialization
{
    private static final String sep = ",";
    private static final Serializer serializer = new Serializer(DocHierarchy.class);
    private static final Deserializer deserializer = new Deserializer(DocHierarchy.class);

    public static String serializeRecord(DocHierarchy value)
    {
        return value.getParentDocUuid() + sep + value.getDocUuid() + sep +
                value.getDataFromTs().getMillis() + sep + value.getDataToTs().getMillis() + sep +
                value.getHierOrder() + sep + value.getClientUuid();
    }

    public static DocHierarchy deserializeRecord(String condensed)
    {
        String[] parts = condensed.split(sep, 6);
        return new DocHierarchyImpl(
                parts[0],
                parts[1],
                TypeConverterUtils.millisToDateTime(Long.parseLong(parts[2])),
                TypeConverterUtils.millisToDateTime(Long.parseLong(parts[3])),
                Integer.parseInt(parts[4]),
                parts[5]);
    }

    public static class Serializer extends StdSerializer<DocHierarchy>
    {
        public static Serializer getInstance()
        {
            return serializer;
        }

        public Serializer(Class<DocHierarchy> t)
        {
            super(t);
        }

        @Override
        public void serialize(DocHierarchy value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException
        {
            jgen.writeString(serializeRecord(value));
        }
    }

    public static class Deserializer extends StdDeserializer<DocHierarchy>
    {
        public static Deserializer getInstance()
        {
            return deserializer;
        }

        public Deserializer(Class<DocHierarchy> vc)
        {
            super(vc);
        }

        @Override
        public DocHierarchy deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            return deserializeRecord(jp.getValueAsString());
        }
    }
}
