package com.teza.common.tardis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.mrbean.MrBeanModule;
import com.google.common.io.Files;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.teza.common.datasvcs.client.ClientContextImpl;
import com.teza.common.datasvcs.jax.AbstractWebClient;
import com.teza.common.datasvcs.jax.RetryFilter;
import com.teza.common.tardis.datatypes.DataType;
import com.teza.common.tardis.datatypes.DataTypeFactory;
import com.teza.common.tardis.deprecated.SearchResult;
import com.teza.common.tardis.handlers.HandlerResult;
import com.teza.common.tardis.handlers.IndexResult;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;

import javax.ws.rs.core.Cookie;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: robin
 * Date: 9/23/16
 * Time: 10:07 AM
 */
public class TardisWebClient extends AbstractWebClient implements IncludeDeprecatedTardisService, Delegatable<TardisClient>
{
    private static final String BOUNDARY = "______BOUNDARY______";
    private static final String CRLF = "\r\n";
    public static final int MAX_TIMEOUT = 60000;
    public static final int RETRY_TIMEOUT = 5000;

    private final Cookie sessionKey;
    private final LocationSelector locationSelector;
    private final DateTime knowledgeTs, freezeTs;
    private final int uploadLimit;
    private final int connectTimeout, readTimeoutInMillis, retryTimeoutInMillis;
    private TardisClient delegate;
    private Client userClient = null;
    private UploadLocationSelector uploadLocationSelector;

    public TardisWebClient(String host, int port, Cookie sessionKey)
    {
        this(host, port, sessionKey, MAX_TIMEOUT, MAX_TIMEOUT, RETRY_TIMEOUT);
    }

    public TardisWebClient(String host, int port, Cookie sessionKey, int connectTimeout, int readTimeoutInMillis, int retryTimeoutInMillis)
    {
        this(host, port, sessionKey, null, null, null, 2, connectTimeout, readTimeoutInMillis, retryTimeoutInMillis);
    }

    public TardisWebClient(String host, int port, Cookie sessionKey, DateTime knowledgeTs, DateTime freezeTs, UploadLocationSelector uploadLocationSelector, int uploadLimit,
                           int connectTimeout, int readTimeoutInMillis, int retryTimeoutInMillis)
    {
        super(host, port, false, "/tardis", new ClientContextImpl(null));
        this.sessionKey = sessionKey;
        getJaxClient().addFilter(new RetryFilter(false));
        getObjectMapper().registerModule(new MrBeanModule());
        if (knowledgeTs == null)
        {
            knowledgeTs = new DateTime(DateTimeZone.UTC);
        }
        this.knowledgeTs = knowledgeTs;
        this.freezeTs = freezeTs;
        this.locationSelector = new LocationSelectorImpl();
        this.uploadLocationSelector = uploadLocationSelector;
        this.uploadLimit = uploadLimit;
        this.connectTimeout = connectTimeout;
        this.readTimeoutInMillis = readTimeoutInMillis;
        this.retryTimeoutInMillis = retryTimeoutInMillis;
        this.delegate = this;
    }


    @Override
    public void setDelegate(TardisClient delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void dispose()
    {
    }

    @Override
    public ObjectMapper createObjectMapper()
    {
        return TardisUtils.registerSerializationModules(new ObjectMapper());
    }

    @Override
    public boolean createDoc(Doc doc)
    {
        if (userClient == null)
        {
            userClient = ClientImpl.getInstance();
            delegate.createClient(userClient);
        }
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/create_doc?" +
                    "source=" + doc.getSource() + "&" +
                    "uuid=" + doc.getDocUuid() + "&" +
                    "client_uuid=" + doc.getClientUuid() + "&" +
                    "data_cls=" + URLEncoder.encode(doc.getDataCls(), "UTF-8") + "&" +
                    "data_cls_version=" + doc.getDataClsVersion() + "&" +
                    "env=" + doc.getEnv() + "&" +
                    "file_pattern=" + URLEncoder.encode(doc.getFilePattern(), "UTF-8") + "&" +
                    "unique_keys=" + URLEncoder.encode(doc.getUniqueKeys(), "UTF-8")
            ).toURI());
            return Boolean.parseBoolean(resource.cookie(sessionKey).get(String.class));
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Doc getDoc(String uuid)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/doc/" + uuid).toURI());
            return resource.cookie(sessionKey).get(Doc.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Doc> searchDocs(JSONObject query)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/search?query=" + URLEncoder.encode(query.toString(), "UTF-8")).toURI());
            return resource.cookie(sessionKey).get(new GenericType<List<Doc>>(){});
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getDistinctDocFields(JSONObject query, String field)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl(
                    String.format("/rest/fieldsearch?field=%s&query=%s",
                            URLEncoder.encode(field, "UTF-8"),
                            URLEncoder.encode(query.toString(), "UTF-8"))).toURI());
            return resource.cookie(sessionKey).get(new GenericType<List<String>>(){});
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<DocAttribute> searchDocAttributes(JSONObject query, boolean history)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl(
                    String.format("/rest/attr/search?history=%squery=%s",
                            history,
                            URLEncoder.encode(query.toString(), "UTF-8"))).toURI());
            return resource.cookie(sessionKey).get(new GenericType<List<DocAttribute>>(){});
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createClient(Client client)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/create_client?" +
                    "uuid=" + client.getClientUuid() + "&" +
                    "user_name=" + URLEncoder.encode(client.getUserName(), "UTF-8") + "&" +
                    "host_name=" + URLEncoder.encode(client.getHostName(), "UTF-8") + "&" +
                    "env=" + client.getEnv() + "&" +
                    "app_version=" + URLEncoder.encode(client.getAppVersion(), "UTF-8")
            ).toURI());
            resource.cookie(sessionKey).get(String.class);
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Client getClient(String uuid)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/client/" + uuid).toURI());
            return resource.cookie(sessionKey).get(Client.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createLocation(FileLocation fileLocation, boolean force)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String delete(String docUuid, String fileUuid, String fileLocationUuid)
    {
        WebResource resource;
        try
        {
            String url = path(null, "rest/delete", docUuid, fileUuid, fileLocationUuid);
            resource = getJaxClient().resource(getUrlService().resolveUrl(url).toURI());
            return resource.cookie(sessionKey).get(String.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean softDeleteFiles(String docUuid, int archivePriority, int uploadLimit)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean softDeleteFiles(String docUuid, String clientUuid)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean unSoftDeleteFiles(String docUuid, String clientUuid)
    {
        throw new RuntimeException("not implemented");

    }

    @Override
    public List<FileLocationRel> getSoftDeletedFiles(String docUuid)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean purgeDoc(String docUuid)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void logAccess(String docUuid, String clientUuid) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public List<UploadLocation> getUploadLocations()
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/list_uploads").toURI());
            return resource.cookie(sessionKey).get(new GenericType<List<UploadLocation>>(){});
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setDocAttribute(String docUuid, String name, String value, String clientUuid)
    {
        if (value == null || name == null)
        {
            throw new RuntimeException("cannot set " + docUuid + " attribute " + name + " to null");
        }

        WebResource resource;
        try
        {
            final String restPath = String.format("rest/attr/set/%s/%s?name=%s&value=%s",
                    docUuid,
                    clientUuid,
                    URLEncoder.encode(name, "UTF-8"),
                    URLEncoder.encode(value, "UTF-8"));
            resource = getJaxClient().resource(getUrlService().resolveUrl(restPath).toURI());
            return Boolean.parseBoolean(resource.cookie(sessionKey).get(String.class));
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean clearDocAttribute(String docUuid, String name, String clientUuid)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl(String.format("rest/attr/clear/%s/%s?name=%s",
                    docUuid,
                    clientUuid,
                    URLEncoder.encode(name, "UTF-8"))).toURI());
            return resource.cookie(sessionKey).get(Boolean.class);
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocAttribute getDocAttribute(String docUuid, String name, DateTime knowledge)
    {
        WebResource resource;
        try
        {
            final String restPath = String.format("rest/attr/get/%s?name=%s%s",
                    docUuid,
                    URLEncoder.encode(name, "UTF-8"),
                    (knowledge == null) ? "" : "&knowledge=" + TardisUtils.formatDateTime(knowledge));
            resource = getJaxClient().resource(getUrlService().resolveUrl(restPath).toURI());
            return resource.cookie(sessionKey).get(DocAttribute.class);
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getDocAttributes(String docUuid, DateTime knowledge)
    {
        WebResource resource;
        try
        {
            final String restPath = String.format("rest/attr/all/%s%s",
                    docUuid,
                    (knowledge == null) ? "" : "?knowledge=" + TardisUtils.formatDateTime(knowledge));
            resource = getJaxClient().resource(getUrlService().resolveUrl(restPath).toURI());
            return resource.cookie(sessionKey).get(new GenericType<Map<String, String>>(){});
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<DocAttributeType> getAttributeTypes()
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("rest/attrtype/list").toURI());
            return resource.cookie(sessionKey).get(new GenericType<List<DocAttributeType>>(){});
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DateTime getDefaultKnowledgeTs()
    {
        return knowledgeTs;
    }

    @Override
    public DateTime getDefaultFreezeTs()
    {
        return freezeTs;
    }

    @Override
    public void addUploadLocation(String fileLocationUuid, String uploadUrl, String permittedSources)
    {
        WebResource resource;
        try
        {
            String queryString = "upload_url=" + URLEncoder.encode(uploadUrl, "UTF-8");
            if (permittedSources != null && !permittedSources.isEmpty())
            {
                queryString += "&permitted_sources=" + URLEncoder.encode(permittedSources, "UTF-8");
            }
            resource = getJaxClient().resource(getUrlService().resolveUrl(path(queryString, "rest/add_upload", fileLocationUuid)).toURI());
            resource.cookie(sessionKey).get(String.class);
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeUploadLocation(String fileLocationUuid)
    {
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/remove_upload/" + fileLocationUuid).toURI());
            resource.cookie(sessionKey).get(String.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDocHierarchy(List<DocHierarchy> docHierarchies)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public DocHierarchyResult getDocHierarchy(String docUuid, DateTime knowledge)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public void createDocAttributeType(String name, String valueType, String description, String clientUuid)
    {
        if (name == null)
        {
            throw new RuntimeException("name cannot be null");
        }

        WebResource resource;
        try
        {
            final String restPath = String.format("rest/attrtype/create?client_uuid=%s&name=%s%s%s",
                    clientUuid,
                    URLEncoder.encode(name, "UTF-8"),
                    (valueType == null) ? "" : "&value_type=" + valueType,
                    (description == null) ? "" : "&description=" + URLEncoder.encode(description, "UTF-8"));
            resource = getJaxClient().resource(getUrlService().resolveUrl(restPath).toURI());
            resource.cookie(sessionKey).get(String.class);
        }
        catch(URISyntaxException | UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocAttributeType getDocAttributeType(String name)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public FileLocation getLocation(String uuid, boolean force)
    {
        WebResource resource;
        try
        {
            if (force)
            {
                uuid += "?force=true";
            }
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/location/" + uuid).toURI());
            return resource.get(FileLocation.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end)
    {
        return get(doc, start, end, getDefaultKnowledgeTs(), getDefaultFreezeTs());
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze)
    {
        return get(doc, start, end, knowledge, freeze, false);
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, boolean force)
    {
        return get(doc, start, end, knowledge, freeze, null, null, force);
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, Method method, IndexResult ir, boolean force)
    {
        if (freeze == null || !freeze.isBefore(knowledge))
        {
            freeze = knowledge;
            if (method == null)
            {
                method = Method.LOOK_BEHIND;
            }
        }
        else
        {
            if (method == null)
            {
                method = Method.LOOK_AHEAD;
            }
        }

        if (ir == null)
        {
            ir = getIndex(doc.getDocUuid(), start, end, freeze,
                    freeze.isBefore(knowledge) ? Method.LOOK_AHEAD : Method.LOOK_BEHIND, force);
        }

        if (ir.isEmpty())
        {
            return null;
        }

        HandlerResult hr = TardisUtils.getHandler(method).handle(doc.getDocUuid(), start, end, knowledge, freeze, ir.getIndexRecords());
        return new TardisResultImpl(doc, hr, ir.getDocs(), ir.getLocations(), locationSelector);
    }

    @Override
    public void indexFile(String fileUUID, String fileLocationUUID, String clientUUID, String docUUID, String hash,
                          DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force, boolean archive)
    {
        WebResource resource;
        try
        {

            final String queryString;
            if (fileMeta == null || fileMeta.isEmpty())
            {
                queryString = "force=" + force + "&archive=" + archive;
            }
            else
            {
                fileMeta = URLEncoder.encode(fileMeta, "UTF-8");
                queryString = "force=" + force + "&archive=" + archive + "&meta=" + fileMeta;
            }

            final String restPath = path(queryString, fileUUID, fileLocationUUID, docUUID, clientUUID, TardisUtils.formatDateTime(start),
                    TardisUtils.formatDateTime(end), TardisUtils.formatDateTime(knowledgeTime), hash);
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/index" + restPath).toURI());
            String ok = resource.cookie(sessionKey).get(String.class);
            if(! "OK".equals(ok))
            {
                throw new Exception("failed to index file: " + restPath);
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void indexContent(String clientUUID, String docUUID, String content, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force)
    {
        WebResource resource;
        try
        {

            String queryString = "force=" + force + (content != null ? "&content=" + URLEncoder.encode(content, "UTF-8") : "");
            if (fileMeta != null && fileMeta.isEmpty())
            {
                queryString = "&meta=" + URLEncoder.encode(fileMeta, "UTF-8");
            }

            final String restPath = path(queryString, docUUID, clientUUID, TardisUtils.formatDateTime(start),
                    TardisUtils.formatDateTime(end), TardisUtils.formatDateTime(knowledgeTime));
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/index_content" + restPath).toURI());
            String ok = resource.cookie(sessionKey).get(String.class);
            if(! "OK".equals(ok))
            {
                throw new Exception("failed to index content: " + restPath);
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexResult getIndex(String docUuid, DateTime start, DateTime end, DateTime knowledge, Method method)
    {
        return getIndex(docUuid, start, end, knowledge, method, false);
    }

    private IndexResult getIndex(String docUuid, DateTime start, DateTime end, DateTime knowledge, Method method, boolean force)
    {
        WebResource resource;
        try
        {
            String url = path(force ? "force=true" : null, "rest/getindex", method.name(), docUuid,
                    TardisUtils.formatDateTime(start), TardisUtils.formatDateTime(end),
                    TardisUtils.formatDateTime(knowledge));
            resource = getJaxClient().resource(getUrlService().resolveUrl(url).toURI());
            return resource.cookie(sessionKey).get(IndexResult.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexResult getIndex(String docUUID, String fileUUID)
    {
        WebResource resource;
        try
        {
            String url = path(null, "rest/getfile", docUUID, fileUUID);
            resource = getJaxClient().resource(getUrlService().resolveUrl(url).toURI());
            return resource.cookie(sessionKey).get(IndexResult.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexResult getIndex(String docUUID)
    {
        WebResource resource;
        try
        {
            String url = path(null, "rest/getallfile", docUUID);
            resource = getJaxClient().resource(getUrlService().resolveUrl(url).toURI());
            return resource.cookie(sessionKey).get(IndexResult.class);
        }
        catch(URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String path(String queryString, String ... paths)
    {
        StringBuilder sb = new StringBuilder();
        for(String path : paths)
        {
            sb.append('/').append(path);
        }
        if (queryString != null && !queryString.isEmpty())
        {
            sb.append("?").append(queryString);
        }

        return sb.toString();
    }

    @Override
    public <T> void put(Doc doc, T value, DateTime start, DateTime end, DateTime knowledge)
    {
        delegate.createDoc(doc);

        DataType<T> dt = DataTypeFactory.getDataType(doc);
        if (dt.isNull(value))
        {
            throw new IllegalArgumentException("cannot put a null value into tardis: " + value);
        }
        value = dt.clean(value, start, end);
        JSONObject fileMeta = dt.getMeta(value);
        if (fileMeta == null)
        {
            fileMeta = new JSONObject();
        }
        File dumpFile;
        try
        {
            dumpFile = File.createTempFile("tardis", ".tmp");
            dt.dumps(value, new FileOutputStream(dumpFile));
            fileMeta.put("file_size", dumpFile.length());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        Set<String> postedDataCenter = new HashSet<String>();
        String body = getPostBody(doc, start, end, knowledge, fileMeta, dumpFile);
        List<UploadLocationSelector.DataCenterLocations> locations = getUploadLocationSelector(false).getLocations(doc.getSource(), null);
        List<String> errorMsgs = post(body, dumpFile, locations, uploadLimit, postedDataCenter,
                connectTimeout, readTimeoutInMillis, retryTimeoutInMillis
        );
        if (errorMsgs != null)
        {
            locations = getUploadLocationSelector(true).getLocations(doc.getSource(), postedDataCenter);
            errorMsgs = post(body, dumpFile, locations, uploadLimit - postedDataCenter.size(), postedDataCenter,
                    connectTimeout, readTimeoutInMillis, retryTimeoutInMillis
            );
        }
        if (errorMsgs != null)
        {
            throw new UploadException("uploaded only to " + postedDataCenter + ": " + errorMsgs);
        }
        //noinspection ResultOfMethodCallIgnored
        dumpFile.delete();
    }

    private String getPostBody(Doc doc, DateTime start, DateTime end, DateTime knowledge, JSONObject fileMeta, File dumpFile)
    {
        String hash = TardisUtils.hashFile(doc.getFilePattern(), dumpFile);
        String fileUuid = TardisUtils.getFileUuid(doc.getDocUuid(), hash, start, end);
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("file_uuid", fileUuid);
        fields.put("client_uuid", userClient.getClientUuid());
        fields.put("doc_uuid", doc.getDocUuid());
        fields.put("file_meta", fileMeta == null ? "" : fileMeta.toString());
        fields.put("start_ts", TardisUtils.formatDateTime(start));
        fields.put("end_ts", TardisUtils.formatDateTime(end));
        fields.put("knowledge_ts", TardisUtils.formatDateTime(knowledge));

        StringBuilder writer = new StringBuilder();
        writer.append("--").append(BOUNDARY).append(CRLF);

        for (String param : fields.keySet())
        {
            writer.append("Content-Disposition: form-data; name=\"").append(param).append("\"").append(CRLF);
            writer.append(CRLF).append(fields.get(param)).append(CRLF);
            writer.append("--").append(BOUNDARY).append(CRLF);
        }

        // send file
        writer.append("Content-Disposition: form-data; name=\"").append(dumpFile.getName()).append("\"; filename=\"tardisupload\"").append(CRLF);
        writer.append("Content-Type: application/octet-stream").append(CRLF);
        writer.append("Content-Transfer-Encoding: binary").append(CRLF);
        writer.append(CRLF);
        return writer.toString();
    }

    private static int postFile(String body, File dumpFile, List<UploadLocation> locs, int limit,
                                int connectTimeoutInMillis, int readTimeoutInMillis, int retryTimeoutInMillis)
    {
        Set<String> seenServers = new HashSet<String>();
        int count = 0, total = 0;
        List<String> errorMsgs = new ArrayList<String>();
        String response;
        for (UploadLocation ul : locs)
        {
            FileLocation loc = ul.getLocation();
            if (seenServers.contains(loc.getServer()))
            {
                continue;
            }
            seenServers.add(loc.getServer());
            total++;

            response = postFile(body, dumpFile, ul, connectTimeoutInMillis, readTimeoutInMillis);
            if (response.equals("OK"))
            {
                count++;
                if (count == limit)
                    break;
                continue;
            }

            if (response.startsWith("post timed out"))
            {
                response = postFile(body, dumpFile, ul, retryTimeoutInMillis, retryTimeoutInMillis);
                if (response.equals("OK"))
                {
                    count++;
                    if (count == limit)
                        break;
                    continue;
                }
            }
            else if (response.contains("expected file size "))
            {
                // if for some reason a partial file was shipped, retry once
                response = postFile(body, dumpFile, ul, connectTimeoutInMillis, readTimeoutInMillis);
                if (response.equals("OK"))
                {
                    count++;
                    if (count == limit)
                        break;
                    continue;
                }
            }

            errorMsgs.add(response);
        }
        if (limit <= total && count < limit)
        {
            throw new UploadException("uploaded only to " + count + " instead of " + limit + " servers: " + errorMsgs);
        }
        return count;
    }

    private static String postFile(String body, File dumpFile, UploadLocation ul, int connectionTimeoutInMillis, int readTimeoutInMillis)
    {
        try
        {
            String url = ul.getUploadUrl() + "?fu=" + TardisUtils.generateInt();
            URLConnection connection = new URL(url).openConnection();
            HttpURLConnection conn = ((HttpURLConnection) connection);
            conn.setConnectTimeout(connectionTimeoutInMillis);
            conn.setReadTimeout(readTimeoutInMillis);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);
            OutputStream output = connection.getOutputStream();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, "UTF-8"), true);
            writer.append(body);
            writer.flush();
            Files.copy(dumpFile, output);
            output.flush(); // Important before continuing with writer!
            writer.append(CRLF).flush(); // CRLF is important! It indicates end of boundary.

            // end of request
            writer.append("--").append(BOUNDARY).append("--").append(CRLF).flush();
            int responseCode = conn.getResponseCode();
            InputStream in;
            if (responseCode >= 200 && responseCode < 300)
            {
                in = conn.getInputStream();
            }
            else
            {
                in = conn.getErrorStream();
            }
            String encoding = conn.getContentEncoding();
            encoding = encoding == null ? "UTF-8" : encoding;
            return IOUtils.toString(in, encoding);
        }
        catch (SocketTimeoutException e)
        {
            return "post timed out: " + e.getMessage();
        }
        catch (IOException e)
        {
            return e.getMessage();
        }
    }

    private static List<String> post(String body, File dumpFile, List<UploadLocationSelector.DataCenterLocations> locations, int limit, Set<String> seenDataCenters,
                                     int connectTimeoutInMillis, int readTimeoutInMillis, int retryTimeoutInMillis)
    {
        List<String> errorMsgs = new ArrayList<String>();

        if (limit > locations.size())
        {
            limit = locations.size();
        }

        for (UploadLocationSelector.DataCenterLocations dcl : locations)
        {
            if (seenDataCenters.size() == limit)
            {
                break;
            }

            if (seenDataCenters.contains(dcl.dataCenter))
            {
                continue;
            }

            try
            {
                if (postFile(body, dumpFile, dcl.locations, 1, connectTimeoutInMillis, readTimeoutInMillis, retryTimeoutInMillis) == 1)
                {
                    seenDataCenters.add(dcl.dataCenter);
                    if (seenDataCenters.size() >= limit)
                    {
                        break;
                    }
                }
            }
            catch (UploadException ignore)
            {
                errorMsgs.add(dcl.dataCenter + " failed: " + ignore.getMessage());
            }
        }
        if (seenDataCenters.size() < limit)
        {
            return errorMsgs;
        }
        return null;
    }

    public static class UploadException extends RuntimeException
    {
        public UploadException(String e)
        {
            super(e);
        }
    }

    protected UploadLocationSelector getUploadLocationSelector(boolean force)
    {
        if (uploadLocationSelector == null || force)
        {
            uploadLocationSelector = new UploadLocationSelector(delegate.getUploadLocations());
        }
        return uploadLocationSelector;
    }

    @Override
    public void putContent(Doc doc, Object value, DateTime start, DateTime end, DateTime knowledge)
    {
        delegate.createDoc(doc);

        DataType dt = DataTypeFactory.getDataType(doc);
        if (!dt.isContent(value))
        {
            throw new IllegalArgumentException("cannot put a null value into tardis: " + value);
        }
        value = dt.cleanContent(value, start, end);
        JSONObject fileMeta = dt.getContentMeta(value);
        indexContent(userClient.getClientUuid(), doc.getDocUuid(), dt.dumpContent(value),
                start, end, knowledge, fileMeta == null ? null : fileMeta.toString(), false);
    }

    @Override
    public void clear(Doc doc, DateTime start, DateTime end, DateTime knowledge)
    {
        delegate.createDoc(doc);

        indexContent(userClient.getClientUuid(), doc.getDocUuid(), null,
                start, end, knowledge, null, true);
    }

    @Override
    public SearchResult lookAhead(String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        throw new RuntimeException("deprecated");
    }

    @Override
    public SearchResult lookBehind(String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        throw new RuntimeException("deprecated");
    }

}
