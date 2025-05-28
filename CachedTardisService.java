package com.teza.common.tardis;

import com.google.common.cache.CacheLoader;
import com.teza.common.codegen.pojo.PojoFactory;
import com.teza.common.tardis.caches.TenureCache;
import com.teza.common.tardis.handlers.IndexResult;
import com.teza.common.tardis.deprecated.SearchResult;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * User: robin
 * Date: 11/23/16
 * Time: 9:30 AM
 */

/**
 * keys by tenure:
 *
 * SHORT:
 * -- getIndex
 * getKeyForQuery(method.name(), docUUID, start, end, knowledge)
 * new StringStringStringStringStringTupleImpl(method, docUuid, start, end, knowledge)
 * "docfile" + docUuid + fileUuid
 * docUuid + fileUuid
 * "allfile" + docUuid
 * docUuid
 * getKeyForQuery("lookAhead", docUUID, start, end, knowledge)
 * getKeyForQuery("lookBehind", docUUID, start, end, knowledge)
 * -- getUploadLocations
 * "uploadServices"
 * -- getDocAttributes
 * "attr" + docUuid + name + knowledge
 * "attrs" + docUuid + knowledge
 * -- getDocHierarchy
 * "hier" + docUuid + knowledge.getMillis()
 *
 * MEDIUM:
 * -- createDoc/getDoc
 * docUuid
 *
 * LONG:
 * clientUuid
 * "attrtypes"
 *
 * FOREVER:
 * locationUuid
 * "attrtype" + name
 *
 */
public class CachedTardisService implements IncludeDeprecatedTardisService
{
    private final TenureCache cache;
    private final IncludeDeprecatedTardisService target;

    public CachedTardisService(IncludeDeprecatedTardisService target, TenureCache cache)
    {
        this.cache = cache;
        this.target = target;
        if (target instanceof Delegatable)
        {
            //noinspection unchecked
            ((Delegatable) target).setDelegate(this);
        }
    }

    private <K,V> V get(TenureCache.Tenure tenure, K key, Callable<V> handler)
    {
        try
        {
            //noinspection unchecked
            return (V)cache.get(tenure, key, handler);
        }
        catch(CacheLoader.InvalidCacheLoadException icle)
        {
            throw icle;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createDoc(final Doc doc)
    {
        final boolean[] created = new boolean[1];
        get(TenureCache.Tenure.MEDIUM, doc.getDocUuid(), () -> {
            created[0] = target.createDoc(doc);
            return doc;
        });
        return created[0];
    }

    @Override
    public Doc getDoc(final String uuid)
    {
        return get(TenureCache.Tenure.MEDIUM, uuid, () -> {
            return target.getDoc(uuid);
        });
    }


    @Override
    public List<Doc> searchDocs(final JSONObject query)
    {
        return target.searchDocs(query);
    }

    @Override
    public List<String> getDistinctDocFields(JSONObject query, String field)
    {
        return target.getDistinctDocFields(query, field);
    }

    @Override
    public List<DocAttribute> searchDocAttributes(JSONObject query, boolean history)
    {
        return target.searchDocAttributes(query, history);
    }

    private QueryKey getKeyForQuery(String type, String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        return PojoFactory.make(QueryKey.class).setType(type).setDocUUID(docUUID).setStart(start).setEnd(end).setKnowledge(knowledge);
    }

    @Override
    public Client getClient(final String uuid)
    {
        return get(TenureCache.Tenure.LONG, uuid, () -> {
            return target.getClient(uuid);
        });
    }

    @Override
    public void createClient(final Client client)
    {
        get(TenureCache.Tenure.LONG, client.getClientUuid(), () -> {
            target.createClient(client);
            return client;
        });
    }

    @Override
    public void createLocation(final FileLocation fileLocation, boolean force)
    {
        if (force)
        {
            //noinspection unchecked
            cache.invalidate(TenureCache.Tenure.FOREVER, fileLocation.getFileLocationUuid());
        }
        get(TenureCache.Tenure.FOREVER, fileLocation.getFileLocationUuid(), () -> {
            target.createLocation(fileLocation, true);
            return fileLocation;
        });
    }

    @Override
    public FileLocation getLocation(final String uuid, boolean force)
    {
        if (force)
        {
            //noinspection unchecked
            cache.invalidate(TenureCache.Tenure.FOREVER, uuid);
        }
        return get(TenureCache.Tenure.FOREVER, uuid, () -> {
            return target.getLocation(uuid, true);
        });
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, boolean force)
    {
        return target.get(doc, start, end, knowledge, freeze, force);
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze)
    {
        return target.get(doc, start, end, knowledge, freeze);
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, Method method, IndexResult ir, boolean force)
    {
        return target.get(doc, start, end, knowledge, freeze, method, ir, force);
    }

    @Override
    public TardisResult get(Doc doc, DateTime start, DateTime end)
    {
        return target.get(doc, start, end);
    }

    @Override
    public <T> void put(Doc doc, T value, DateTime start, DateTime end, DateTime knowledge)
    {
        target.put(doc, value, start, end, knowledge);
    }

    @Override
    public void putContent(Doc doc, Object value, DateTime start, DateTime end, DateTime knowledge)
    {
        target.putContent(doc, value, start, end, knowledge);
    }

    @Override
    public void clear(Doc doc, DateTime start, DateTime end, DateTime knowledge)
    {
        target.clear(doc, start, end, knowledge);
    }

    @Override
    public void indexFile(String fileUUID, String fileLocationUuid, String clientUUID, String docUUID, String hash, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force, boolean archive)
    {
        target.indexFile(fileUUID, fileLocationUuid, clientUUID, docUUID, hash, start, end, knowledgeTime, fileMeta, force, archive);
    }

    @Override
    public void indexContent(String clientUUID, String docUUID, String content, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force)
    {
        target.indexContent(clientUUID, docUUID, content, start, end, knowledgeTime, fileMeta, force);
    }

    @Override
    public IndexResult getIndex(final String docUUID, final DateTime start, final DateTime end, final DateTime knowledge, final Method method)
    {
        try
        {
            // final Tuple key = new StringStringStringStringStringTupleImpl(method, docUuid, start, end, knowledge);
            return get(TenureCache.Tenure.SHORT, getKeyForQuery(method.name(), docUUID, start, end, knowledge), () -> {
                return target.getIndex(docUUID, start, end, knowledge, method);
            });
        }
        catch(CacheLoader.InvalidCacheLoadException icle)
        {
            return PojoFactory.make(IndexResult.class);
        }
    }

    @Override
    public IndexResult getIndex(final String docUUID, final String fileUUID)
    {
        try
        {
            // final String key = docUuid + fileUuid;
            return get(TenureCache.Tenure.SHORT, "docfile" + docUUID + fileUUID, () -> {
                return target.getIndex(docUUID, fileUUID);
            });
        }
        catch(CacheLoader.InvalidCacheLoadException icle)
        {
            return PojoFactory.make(IndexResult.class);
        }
    }

    @Override
    public IndexResult getIndex(final String docUUID)
    {
        try
        {
            // final String key = docUUID;
            return get(TenureCache.Tenure.SHORT, "allfile" + docUUID, () -> {
                return target.getIndex(docUUID);
            });
        }
        catch(CacheLoader.InvalidCacheLoadException icle)
        {
            return PojoFactory.make(IndexResult.class);
        }
    }

    @Override
    public SearchResult lookAhead(final String docUUID, final DateTime start, final DateTime end, final DateTime knowledge)
    {
        try
        {
            return get(TenureCache.Tenure.SHORT, getKeyForQuery("lookAhead", docUUID, start, end, knowledge), () -> {
                //noinspection deprecation
                return target.lookAhead(docUUID, start, end, knowledge);
            });
        }
        catch(CacheLoader.InvalidCacheLoadException icle)
        {
            return PojoFactory.make(SearchResult.class);
        }
    }

    @Override
    public SearchResult lookBehind(final String docUUID, final DateTime start, final DateTime end, final DateTime knowledge)
    {
        try
        {
            return get(TenureCache.Tenure.SHORT, getKeyForQuery("lookBehind", docUUID, start, end, knowledge), () -> {
                //noinspection deprecation
                return target.lookBehind(docUUID, start, end, knowledge);
            });
        }
        catch(CacheLoader.InvalidCacheLoadException icle)
        {
            return PojoFactory.make(SearchResult.class);
        }
    }

    @Override
    public String delete(String docUuid, String fileUuid, String fileLocationUuid)
    {
        return target.delete(docUuid, fileUuid, fileLocationUuid);
    }

    @Override
    public boolean softDeleteFiles(String docUuid, int archivePriority, int uploadLimit)
    {
        return target.softDeleteFiles(docUuid, archivePriority, uploadLimit);
    }

    @Override
    public boolean softDeleteFiles(String docUuid, String clientUuid)
    {
        return target.softDeleteFiles(docUuid, clientUuid);
    }

    @Override
    public boolean unSoftDeleteFiles(String docUuid, String clientUuid)
    {
        return target.unSoftDeleteFiles(docUuid, clientUuid);
    }

    @Override
    public List<FileLocationRel> getSoftDeletedFiles(String docUuid)
    {
        return target.getSoftDeletedFiles(docUuid);
    }

    @Override
    public boolean purgeDoc(String docUuid)
    {
        return target.purgeDoc(docUuid);
    }

    @Override
    public void logAccess(String docUuid, String clientUuid) {
        target.logAccess(docUuid, clientUuid);
    }

    @Override
    public List<UploadLocation> getUploadLocations()
    {
        try
        {
            return get(TenureCache.Tenure.SHORT, "uploadServices", target::getUploadLocations);
        }
        catch(CacheLoader.InvalidCacheLoadException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setDocAttribute(String docUuid, String name, String value, String clientUuid)
    {
        return target.setDocAttribute(docUuid, name, value, clientUuid);
    }

    @Override
    public boolean clearDocAttribute(String docUuid, String name, String clientUuid)
    {
        return target.clearDocAttribute(docUuid, name, clientUuid);
    }

    @Override
    public DocAttribute getDocAttribute(final String docUuid, final String name, final DateTime knowledge)
    {
        try
        {
            return get(TenureCache.Tenure.SHORT, "attr" + docUuid + name + (knowledge == null ? "" : knowledge.getMillis()),
                    () -> {
                        //noinspection deprecation
                        return target.getDocAttribute(docUuid, name, knowledge);
                    });
        }
        catch(CacheLoader.InvalidCacheLoadException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getDocAttributes(final String docUuid, final DateTime knowledge)
    {
        try
        {
            return get(TenureCache.Tenure.SHORT, "attrs" + docUuid + (knowledge == null ? "" : knowledge.getMillis()),
                    () -> {
                        //noinspection deprecation
                        return target.getDocAttributes(docUuid, knowledge);
                    });
        }
        catch(CacheLoader.InvalidCacheLoadException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<DocAttributeType> getAttributeTypes()
    {
        try
        {
            return get(TenureCache.Tenure.LONG, "attrtypes", () -> {
                //noinspection deprecation
                List<DocAttributeType> types = target.getAttributeTypes();
                for (DocAttributeType t : types)
                {
                    //noinspection unchecked
                    cache.put(TenureCache.Tenure.FOREVER, "attrtype" + t.getName(), t);
                }
                return types;
            });
        }
        catch(CacheLoader.InvalidCacheLoadException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DateTime getDefaultKnowledgeTs()
    {
        return target.getDefaultKnowledgeTs();
    }

    @Override
    public DateTime getDefaultFreezeTs()
    {
        return target.getDefaultFreezeTs();
    }

    @Override
    public void addUploadLocation(String fileLocationUuid, String uploadUrl, String sources)
    {
        target.addUploadLocation(fileLocationUuid, uploadUrl, sources);
    }

    @Override
    public void removeUploadLocation(String fileLocationUuid)
    {
        target.removeUploadLocation(fileLocationUuid);
    }

    @Override
    public void createDocHierarchy(List<DocHierarchy> docHierarchies)
    {
        target.createDocHierarchy(docHierarchies);
    }

    @Override
    public DocHierarchyResult getDocHierarchy(final String docUuid, final DateTime knowledge)
    {
        try
        {
            return get(TenureCache.Tenure.SHORT, "hier" + docUuid + knowledge.getMillis(), () -> {
                //noinspection deprecation
                return target.getDocHierarchy(docUuid, knowledge);
            });
        }
        catch(CacheLoader.InvalidCacheLoadException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDocAttributeType(final String name, final String valueType, final String description, final String clientUuid)
    {
        //noinspection unchecked
        cache.invalidate(TenureCache.Tenure.LONG, "attrtypes");
        target.createDocAttributeType(name, valueType, description, clientUuid);
    }

    @Override
    public DocAttributeType getDocAttributeType(final String name)
    {
        String key = "attrtype" + name;
        //noinspection unchecked
        DocAttributeType type = (DocAttributeType) cache.getIfPresent(TenureCache.Tenure.FOREVER, key);
        if (type == null)
        {
            type = target.getDocAttributeType(name);
            if (type != null)
            {
                try
                {
                    //noinspection unchecked
                    cache.put(TenureCache.Tenure.FOREVER, key, type);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
        return type;
    }

    @Override
    public void dispose()
    {
        target.dispose();
    }

    public interface QueryKey
    {
        String getType(); QueryKey setType(String value);
        @SuppressWarnings("unused")
        String getDocUUID(); QueryKey setDocUUID(String value);
        DateTime getStart(); QueryKey setStart(DateTime value);
        DateTime getEnd(); QueryKey setEnd(DateTime value);
        DateTime getKnowledge(); QueryKey setKnowledge(DateTime value);
    }

}
