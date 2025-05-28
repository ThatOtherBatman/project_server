package com.teza.common.tardis;

import com.teza.common.tardis.deprecated.SearchResult;
import com.teza.common.tardis.handlers.IndexResult;
import com.teza.common.util.DbFactory;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 1/12/17
 * Time: 11:46 AM
 */
public class LazyTardisServiceDbImpl implements IncludeDeprecatedTardisService, Delegatable<TardisService>
{
    private final DbFactory factory;
    private TardisService delegate = null;
    private TardisServiceDbImpl service = null;

    public LazyTardisServiceDbImpl(DbFactory factory)
    {
        this.factory = factory;
    }

    @Override
    public void setDelegate(TardisService delegate)
    {
        this.delegate = delegate;
        if (service != null)
        {
            service.setDelegate(delegate);
        }
    }

    @Override
    public void dispose()
    {
        if (service != null)
        {
            service.dispose();
            service = null;
        }
    }

    private IncludeDeprecatedTardisService getService()
    {
        if (service == null)
        {
            service = new TardisServiceDbImpl(factory.getConnection());
            if (delegate != null)
            {
                service.setDelegate(delegate);
            }
        }
        return service;
    }

    @Override
    public SearchResult lookAhead(String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        //noinspection deprecation
        return getService().lookAhead(docUUID, start, end, knowledge);
    }

    @Override
    public SearchResult lookBehind(String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        //noinspection deprecation
        return getService().lookBehind(docUUID, start, end, knowledge);
    }

    @Override
    public void indexFile(String fileUUID, String fileLocationUuid, String clientUUID, String docUUID, String hash, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force, boolean archive)
    {
        getService().indexFile(fileUUID, fileLocationUuid, clientUUID, docUUID, hash, start, end, knowledgeTime, fileMeta, force, archive);
    }

    @Override
    public void indexContent(String clientUUID, String docUUID, String content, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force)
    {
        getService().indexContent(clientUUID, docUUID, content, start, end, knowledgeTime, fileMeta, force);
    }

    @Override
    public IndexResult getIndex(String docUUID, DateTime start, DateTime end, DateTime knowledge, Method method)
    {
        return getService().getIndex(docUUID, start, end, knowledge, method);
    }

    @Override
    public IndexResult getIndex(String docUUID, String fileUUID)
    {
        return getService().getIndex(docUUID, fileUUID);
    }

    @Override
    public IndexResult getIndex(String docUUID)
    {
        return getService().getIndex(docUUID);
    }

    @Override
    public boolean createDoc(Doc doc)
    {
        return getService().createDoc(doc);
    }

    @Override
    public Doc getDoc(String uuid)
    {
        return getService().getDoc(uuid);
    }

    @Override
    public List<Doc> searchDocs(JSONObject query)
    {
        return getService().searchDocs(query);
    }

    @Override
    public Client getClient(String uuid)
    {
        return getService().getClient(uuid);
    }

    @Override
    public void createClient(Client client)
    {
        getService().createClient(client);
    }

    @Override
    public void createLocation(FileLocation fileLocation, boolean force)
    {
        getService().createLocation(fileLocation, force);
    }

    @Override
    public String delete(String docUuid, String fileUuid, String fileLocationUuid)
    {
        return getService().delete(docUuid, fileUuid, fileLocationUuid);
    }

    @Override
    public boolean softDeleteFiles(String docUuid, int archivePriority, int uploadLimit)
    {
        return getService().softDeleteFiles(docUuid, archivePriority, uploadLimit);
    }

    @Override
    public boolean softDeleteFiles(String docUuid, String clientUuid)
    {
        return getService().softDeleteFiles(docUuid, clientUuid);
    }

    @Override
    public boolean unSoftDeleteFiles(String docUuid, String clientUuid)
    {
        return getService().unSoftDeleteFiles(docUuid, clientUuid);
    }

    @Override
    public List<FileLocationRel> getSoftDeletedFiles(String docUuid)
    {
        return getService().getSoftDeletedFiles(docUuid);
    }

    @Override
    public boolean purgeDoc(String docUuid)
    {
        return getService().purgeDoc(docUuid);
    }

    @Override
    public void logAccess(String docUuid, String clientUuid) {
        getService().logAccess(docUuid, clientUuid);
    }

    @Override
    public List<UploadLocation> getUploadLocations()
    {
        return getService().getUploadLocations();
    }

    @Override
    public boolean setDocAttribute(String docUuid, String name, String value, String clientUuid)
    {
        return getService().setDocAttribute(docUuid, name, value, clientUuid);
    }

    @Override
    public boolean clearDocAttribute(String docUuid, String name, String clientUuid)
    {
        return getService().clearDocAttribute(docUuid, name, clientUuid);
    }

    @Override
    public DocAttribute getDocAttribute(String docUuid, String name, DateTime knowledge)
    {
        return getService().getDocAttribute(docUuid, name, knowledge);
    }

    @Override
    public Map<String, String> getDocAttributes(String docUuid, DateTime knowledge)
    {
        return getService().getDocAttributes(docUuid, knowledge);
    }

    @Override
    public List<DocAttributeType> getAttributeTypes()
    {
        return getService().getAttributeTypes();
    }

    @Override
    public DateTime getDefaultKnowledgeTs()
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public DateTime getDefaultFreezeTs()
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public void addUploadLocation(String fileLocationUuid, String uploadUrl, String sources)
    {
        getService().addUploadLocation(fileLocationUuid, uploadUrl, sources);
    }

    @Override
    public void removeUploadLocation(String fileLocationUuid)
    {
        getService().removeUploadLocation(fileLocationUuid);
    }

    @Override
    public void createDocHierarchy(List<DocHierarchy> docHierarchies)
    {
        getService().createDocHierarchy(docHierarchies);
    }

    @Override
    public DocHierarchyResult getDocHierarchy(String docUuid, DateTime knowledge)
    {
        return getService().getDocHierarchy(docUuid, knowledge);
    }

    @Override
    public void createDocAttributeType(String name, String valueType, String description, String clientUuid)
    {
        getService().createDocAttributeType(name, valueType, description, clientUuid);
    }

    @Override
    public DocAttributeType getDocAttributeType(String name)
    {
        return getService().getDocAttributeType(name);
    }

    @Override
    public List<String> getDistinctDocFields(JSONObject query, String field)
    {
        return getService().getDistinctDocFields(query, field);
    }

    @Override
    public List<DocAttribute> searchDocAttributes(JSONObject query, boolean history)
    {
        return getService().searchDocAttributes(query, history);
    }

    @Override
    public FileLocation getLocation(String uuid, boolean force)
    {
        return getService().getLocation(uuid, force);
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, boolean force)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, Method method, IndexResult ir, boolean force)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public <T> void put(Doc doc, T value, DateTime start, DateTime end, DateTime knowledge)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public void putContent(Doc doc, Object value, DateTime start, DateTime end, DateTime knowledge)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public void clear(Doc doc, DateTime start, DateTime end, DateTime knowledge)
    {
        throw new RuntimeException("not supported");
    }
}
