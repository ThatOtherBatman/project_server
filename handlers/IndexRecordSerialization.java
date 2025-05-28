package com.teza.common.tardis.handlers;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * User: tom
 * Date: 1/6/17
 * Time: 4:33 PM
 */
@SuppressWarnings("DuplicateThrows")
public class IndexRecordSerialization
{
    private static final String sep = "/";
    private static final Serializer serializer = new Serializer(IndexRecord.class);
    private static final Deserializer deserializer = new Deserializer(IndexRecord.class);

    public static String serializeRecord(IndexRecord value)
    {
        return value.getDocUuid() + sep + value.getHierOrder() + sep +
                value.getParentDocUuid() + sep + value.getParentHierOrder() + sep + value.getStartTs() + sep + value.getEndTs() + sep +
                value.getFileUuid() + sep + value.getValidFromTs() + sep + value.getValidToTs() + sep +
                value.getContent() + sep + value.getDataFromTs() + sep + value.getDataToTs() + sep +
                value.getLocationUuids() + sep + value.getFileMeta();
    }

    public static IndexRecord deserializeRecord(String condensed)
    {
        String[] parts = condensed.split(sep, 14);
        return new IndexRecordImpl(
                parts[0],
                Integer.parseInt(parts[1]),
                parts[2].equals("null") ? null : parts[2],
                Integer.parseInt(parts[3]),
                Long.parseLong(parts[4]),
                Long.parseLong(parts[5]),
                parts[6].equals("null") ? null : parts[6],
                Long.parseLong(parts[7]),
                Long.parseLong(parts[8]),
                parts[9].equals("null") ? null : parts[9],
                Long.parseLong(parts[10]),
                Long.parseLong(parts[11]),
                parts[13].equals("null") ? null : parts[13],
                parts[12].equals("null") ? null : parts[12]
        );
    }

    public static class Serializer extends StdSerializer<IndexRecord>
    {
        public static Serializer getInstance()
        {
            return serializer;
        }

        public Serializer(Class<IndexRecord> t)
        {
            super(t);
        }

        @Override
        public void serialize(IndexRecord value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException
        {
            jgen.writeString(serializeRecord(value));
        }
    }

    public static class Deserializer extends StdDeserializer<IndexRecord>
    {
        public static Deserializer getInstance()
        {
            return deserializer;
        }

        public Deserializer(Class<IndexRecord> vc)
        {
            super(vc);
        }

        @Override
        public IndexRecord deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            return deserializeRecord(jp.getValueAsString());
        }
    }
}
