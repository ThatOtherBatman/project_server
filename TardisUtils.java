package com.teza.common.tardis;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.mrbean.MrBeanModule;
import com.teza.common.datasvcs.serialization.JsonOrgSerializationModule;
import com.teza.common.tardis.handlers.*;
import com.teza.common.util.Datetime;
import com.teza.common.util.MD5Hash;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.teza.common.env.TezaEnvFactory.getTezaEnv;

/**
 * User: tom
 * Date: 10/11/16
 * Time: 4:59 PM
 */

/**
 * collection of utility functions used by various Tardis clients and services
 */
public class TardisUtils
{
    public enum SystemAttributeType
    {
        READ("_read"),
        WRITE("_write"),
        ARCHIVED("_archived"),
        DELETED("_deleted");

        private final String name;

        SystemAttributeType(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    private static final AtomicInteger internalCount = new AtomicInteger(ThreadLocalRandom.current().nextInt(0, 2147483647));
    public static final String DELETED_ATTRIBUTE_VALUE = "__DELETED__";
    public static final String IGNORED_UUID_KEY = "__IGNORED__";
    public static final UUID5 CLIENT_NAMESPACE = new UUID5("tardis.client");
    public static final UUID5 LOCATION_NAMESPACE = new UUID5("tardis.location");
    public static final UUID5 DOC_NAMESPACE = new UUID5("tardis.doc");
    public static final DateTime DEFAULT_START_TS = new DateTime(-2208988800000L, DateTimeZone.UTC);  // 1900-01-01 UTC
    public static final DateTime DEFAULT_END_TS = new DateTime(4102444800000L, DateTimeZone.UTC);  // 2100-01-01 UTC
    private static final DateTimeFormatter ISO_8601_DATE_WITH_TZ_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZ");
    private static final DateTimeFormatter INTEGER_DATE_FORMATTER = DateTimeFormat.forPattern("yyyyMMdd");
    private static final ThreadLocal<MessageDigest> sha1 = new ThreadLocal<MessageDigest>()
    {

        @Override
        protected MessageDigest initialValue()
        {
            try
            {
                return MessageDigest.getInstance("SHA-1");
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new Error(e);
            }
        }

    };

    public static int generateInt()
    {
        try
        {
            return internalCount.getAndIncrement();
        }
        catch (Throwable t)
        {
            internalCount.set(1);
            return 0;
        }
    }

    public static DateTime parseDateTime(String value)
    {
        return ISO_8601_DATE_WITH_TZ_FORMATTER.parseDateTime(value);
    }

    public static DateTime parseDateTime(LocalDate value)
    {
        try
        {
            return new DateTime(new Datetime(Datetime.toStringDate(value) + " 00:00:00 UTC").toMicrosecs() / 1000L, DateTimeZone.UTC);
        }
        catch (ParseException e)
        {
            e.printStackTrace();
            throw new RuntimeException("cannot parse local date " + value + ": " + e);
        }
    }

    public static String formatDateTime(DateTime dt)
    {
        return ISO_8601_DATE_WITH_TZ_FORMATTER.print(dt);
    }

    public static String formatDate(DateTime dt)
    {
        return INTEGER_DATE_FORMATTER.print(dt);
    }

    public static boolean isProd()
    {
        String[] parts = getTezaEnv().getEnvName().split("/", 0);
        return parts[parts.length - 1].equals("PROD");
    }

    public static boolean isValidFileNamePart(String value)
    {
        if (value == null)
        {
            return false;
        }
        boolean valid;
        char[] chars = value.toCharArray();
        for (char c : chars)
        {
            valid = ((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) ||
                    ((c >= '0') && (c <= '9')) ||
                    (c == '_') || (c == '.') || (c == '~') ||
                    (c == '=') || (c == '/') || (c == '-') || (c == '+');
            if (!valid)
                return false;
        }
        return true;
    }

    public static String getFileName(Doc doc, String hash, DateTime startTS, DateTime endTS)
    {
        String filePattern = doc.getFilePattern();
        JSONObject keys = new JSONObject(doc.getUniqueKeys());
        HashMap<String, String> fileFormat = new HashMap<String, String>();
        fileFormat.put("source", doc.getSource());
        fileFormat.put("content", hash);
        fileFormat.put("hash", hash);
        fileFormat.put("hash_prefix", hash.substring(0, 3));
        if (filePattern.contains("{from_date}"))
        {
            fileFormat.put("from_date", TardisUtils.formatDate(startTS));
        }
        if (filePattern.contains("{to_date}"))
        {
            fileFormat.put("to_date", TardisUtils.formatDate(endTS));
        }
        Iterator params = keys.keys();
        while (params.hasNext())
        {
            String k = params.next().toString();
            fileFormat.put(k, keys.get(k).toString());
        }
        return formatKvs(filePattern, fileFormat);
    }

    public static String formatKvs(String string, HashMap<String, String> kvs)
    {
        StrSubstitutor sub = new StrSubstitutor(kvs, "{", "}");
        return sub.replace(string);
    }

    public static IndexHandler getHandler(Method method)
    {
        if (method == null)
            method = Method.LOOK_AHEAD;
        if (method == Method.LOOK_AHEAD)
        {
            return new LookAheadHandler();
        }
        else if (method == Method.LOOK_BEHIND)
        {
            return new LookBehindHandler();
        }
        else if (method == Method.LAST)
        {
            return new LastHandler();
        }
        return new LookAheadHandler();
    }

//    public static int getShard(String uuid)
//    {
//        return Integer.parseInt(uuid.substring(0, 3), 16);
//    }

    private static String getServerPath(FileLocation fl)
    {
        String server = fl.getServer();
        if (server == null || server.isEmpty())
        {
            return "file://" + fl.getLocalParentDir() + (fl.getLocalParentDir().endsWith("/") ? "" : "/");
        }
        String parentDir = fl.getParentDir();
        return "http://" + server + (server.endsWith("/") ? "" : "/") + parentDir + (parentDir.endsWith("/") ? "" : "/");
    }

    public static InputStream getInputStream(String fileName, FileLocation fileLocation)
    {
        String uri = getServerPath(fileLocation) + fileName;
        try
        {
            if (uri.startsWith("file://"))
            {
                return new FileInputStream(uri.substring(7));
            }
            else
            {
                return new URL(uri).openStream();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String getSha1(File file)
    {
        InputStream in = null;
        try
        {
            in = new FileInputStream(file);
            MessageDigest sha1Local = sha1.get();
            sha1Local.reset();
            byte[] buffer = new byte[65536];
            //sha1.digest(buffer, 0, 256);//Need a seed
            while (true)
            {
                int numRead = in.read(buffer, 0, buffer.length);
                if (numRead < 0) break;
                sha1Local.update(buffer, 0, numRead);
                if (numRead < buffer.length) break;
            }

            buffer = sha1Local.digest();
            StringBuilder ret = new StringBuilder();
            for (byte b : buffer)
            {
                ret.append(Integer.toHexString((b >> 4) & 0xf));
                ret.append(Integer.toHexString(b & 0xf));
            }
            return ret.toString();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            throw new Error(e);
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
            throw new Error(t);
        }
        finally
        {
            IOUtils.closeQuietly(in);
        }
    }


    public static String getFileUuid(String docUuid, String hash, DateTime startTs, DateTime endTs)
    {
        long start = startTs.getMillis();
        long end = endTs.getMillis();
        String key = start + "|" + end + "|" + hash;
        return UUID5.getUuid5(key, docUuid);
    }

    public static String hashFile(String filePattern, File file)
    {
        String hash;
        if (filePattern.contains("{hash}"))
        {
            hash = getSha1(file);
            if (hash.length() < 40)
            {
                hash = String.format("%40s", hash).replace(' ', '0');
            }
        }
        else
        {
            hash = MD5Hash.md5(file);
            if (hash.length() < 32)
            {
                hash = String.format("%32s", hash).replace(' ', '0');
            }
        }
        return hash;
    }

    public static String getDocUuid(Source source, String dataCls, String dataClsVersion,
                                  String filePattern, String env, JSONObject uniqueKeys)
    {
        JSONObject values = new JSONObject();
        Iterator iterator = uniqueKeys.keys();
        String key;
        while (iterator.hasNext())
        {
            key = (String) iterator.next();
            if (IGNORED_UUID_KEY.equals(key))
            {
                continue;
            }
            if (uniqueKeys.isNull(key))
            {
                continue;
            }
            Object obj = uniqueKeys.get(key);
            if (obj instanceof Boolean)
            {
                values.put(key, (Boolean) obj ? "True" : "False");
            }
            else
            {
                values.put(key, obj.toString());
            }
        }
        values.put("data_source_id", source.getId())
                .put("data_cls", dataCls)
                .put("data_cls_version", dataClsVersion)
                .put("file_pattern", filePattern)
                .put("env", env);
        return getUuid(values, DOC_NAMESPACE);
    }

    public static String getClientUuid(String userName, String hostName, String env, String appVersion)
    {
        return getUuid(new JSONObject()
                .put("user_name", userName)
                .put("host_name", hostName)
                .put("env", env)
                .put("app_version", appVersion),
                CLIENT_NAMESPACE);
    }

    public static String getFileLocationUuid(String server, String parentDir)
    {
        return getUuid(new JSONObject().put("server", server).put("parent_dir", parentDir), LOCATION_NAMESPACE);
    }

    public static String getUuid(JSONObject values, UUID5 namespace)
    {
        //noinspection unchecked
        List<String> keys = (List<String>) IteratorUtils.toList(values.keys());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String k : keys)
        {
            if (values.isNull(k))
            {
                continue;
            }
            if (!first)
                sb.append("|");
            sb.append(k).append("=").append(values.get(k).toString());
            first = false;
        }
        return new UUID5(sb.toString(), namespace).toString();
    }

    public static ObjectMapper registerSerializationModules(ObjectMapper mapper)
    {
        return mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(new JsonOrgSerializationModule())
                .registerModule(new MrBeanModule())
                .registerModule(new SimpleModule("TardisSerializationModule", new Version(1, 0, 0, null, null, null))
                        .addSerializer(IndexRecord.class, IndexRecordSerialization.Serializer.getInstance())
                        .addSerializer(DocHierarchy.class, DocHierarchySerialization.Serializer.getInstance())
                        .addDeserializer(IndexRecord.class, IndexRecordSerialization.Deserializer.getInstance())
                        .addDeserializer(DocHierarchy.class, DocHierarchySerialization.Deserializer.getInstance()));
    }

}
