package com.teza.common.tardis;

import com.teza.common.db.entity.EntityCrudHandler;
import com.teza.common.db.entity.EntityCrudHandlerFactory;
import com.teza.common.tardis.handlers.IndexRecord;
import com.teza.common.tardis.handlers.IndexRecordImpl;
import com.teza.common.tardis.handlers.IndexResult;
import com.teza.common.tardis.deprecated.SearchResult;
import com.teza.common.util.TypeConverterUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.teza.common.tardis.UUID5.NIL_UUID;

/**
 * User: robin
 * Date: 9/19/16
 * Time: 2:22 PM
 */
@SuppressWarnings("Duplicates")
public class TardisServiceDbImpl implements IncludeDeprecatedTardisService, Delegatable<TardisService>
{
    private final Connection connection;

    private static final String GET_DOC_BY_UUID_SQL = "select * from v_doc where doc_uuid = '{uuid}'";
    private static final String SEARCH_DISTINCT_FIELDS_SQL = "select distinct {field} from v_doc where ";
    private static final String SEARCH_DOC_SQL = "select * from v_doc where ";
    private static final String INSERT_DOC_SQL = "select f_insert_doc('{source}', '{doc_uuid}', '{data_cls}', '{data_cls_version}', '{file_pattern}', '{env}', '{unique_keys}', '{client_uuid}')";

    private static final String GET_CLIENT_BY_UUID_SQL = "select * from client where client_uuid = '{uuid}'";
    private static final String INSERT_CLIENT_SQL =
            "insert into client " +
            "(client_uuid, user_name, host_name, env, app_version) values " +
            "('{uuid}', '{user_name}', '{host_name}', '{env}', '{app_version}') " +
            "on conflict (client_uuid) do nothing";

    private static final String GET_FILE_LOCATION_BY_UUID_SQL = "select * from file_location where file_location_uuid = '{uuid}'";
    private static final String INSERT_FILE_LOCATION_SQL =
            "insert into file_location " +
            "(file_location_uuid, server, parent_dir, local_parent_dir, owner, client_uuid, data_center, priority) values " +
            "('{uuid}', '{server}', '{parent_dir}', '{local_parent_dir}', '{owner}', '00000000-0000-0000-0000-000000000000', '{data_center}', {priority})" +
            "on conflict (file_location_uuid) do update set priority = EXCLUDED.priority, data_center = EXCLUDED.data_center";

    private static final String GET_UPLOAD_LOCATIONS_SQL = "select l.*, u.upload_url, u.sources, u.is_active from upload_service u " +
            "join file_location l on u.file_location_uuid = l.file_location_uuid order by l.priority";
    private static final String INSERT_UPLOAD_LOCATION_SQL = "select f_insert_upload_service('{location_uuid}', '{upload_url}', {sources})";
    private static final String DELETE_UPLOAD_LOCATION_SQL = "update upload_service set is_active = false where file_location_uuid = '{location_uuid}'";

    private static final String INDEX_CONTENT_SQL = "select f_get_or_insert_content('{doc_uuid}', '{client_uuid}', " +
            "'{knowledge_ts}', '{data_from_ts}', '{data_to_ts}', '{content}', {file_meta})";
    private static final String INSERT_CONTENT_SQL = "select f_insert_content('{doc_uuid}', '{client_uuid}', " +
            "'{knowledge_ts}', '{data_from_ts}', '{data_to_ts}', '{content}', {file_meta})";
    private static final String INDEX_FILE_SQL = "select f_get_or_insert_file('{doc_uuid}', '{file_uuid}', '{client_uuid}', " +
            "{location_uuid}, '{knowledge_ts}', '{data_from_ts}', '{data_to_ts}', '{content}', {file_meta})";
    private static final String INSERT_FILE_SQL = "select f_insert_file('{doc_uuid}', '{file_uuid}', '{client_uuid}', " +
            "{location_uuid}, '{knowledge_ts}', '{data_from_ts}', '{data_to_ts}', '{content}', {file_meta})";
    private static final String LOOK_AHEAD_SQL = "select * from f_file_look_ahead(" +
            "'{doc_uuid}', '{knowledge_ts}', '{start_ts}', '{end_ts}')";

    private static final String LOOK_BEHIND_SQL = "select * from f_file_look_behind(" +
            "'{doc_uuid}', '{knowledge_ts}', '{start_ts}', '{end_ts}')";

    private static final String FILE_SQL = "select * from f_file('{doc_uuid}', '{file_uuid}')";
    private static final String ALL_FILE_SQL = "select * from f_file('{doc_uuid}')";
    private static final String CONFLICT_FILES_SQL = "select * from f_conflict_file('{doc_uuid}', '{file_uuid}', '{location_uuid}')";
    private static final String DELETE_FILE_SQL = "delete from deleted_file_location_rel where doc_uuid = '{doc_uuid}' and file_uuid = '{file_uuid}' and file_location_uuid = '{location_uuid}'";

    private static final String GET_DOC_HIER_SQL = "select * from f_doc_hierarchy('{doc_uuid}', '{knowledge}')";
    private static final String GET_DOC_HIER_FOR_UPDATE_SQL = "select parent_doc_uuid, doc_uuid, data_from_ts, data_to_ts, hier_order, client_uuid, doc_hierarchy_id " +
            "from doc_hierarchy where parent_doc_uuid in ({doc_uuids}) for update";
    private static final String DELETE_DOC_HIER_BY_IDS_SQL = "delete from doc_hierarchy where doc_hierarchy_id in ({ids})";
    private static final String INSERT_DOC_HIER_SQL  = "select f_insert_doc_hierarchy(?::uuid, ?::uuid, ?, ?, ?, ?::uuid )";

    private static final String INSERT_DOC_ATTR_TYPE_SQL = "insert into doc_attribute_type (name, value_type, description, client_uuid) values (?, ?, ?, ?::uuid) on conflict (name) do nothing";
    private static final String GET_DOC_ATTR_TYPE_ID_SQL  = "select doc_attribute_type_id, name, value_type from doc_attribute_type where name = ?";

    private static final String INSERT_DOC_ATTR_SQL = "insert into doc_attribute (shard, doc_uuid, doc_attribute_type_id, value, client_uuid) values (f_shard(?::uuid), ?::uuid, ?, ?, ?::uuid) " +
            "on conflict (doc_uuid, doc_attribute_type_id) do " +
            "update set value = EXCLUDED.value, last_updated = current_timestamp, client_uuid = EXCLUDED.client_uuid where doc_attribute.value != EXCLUDED.value";
    private static final String DELETE_DOC_ATTR_SQL = "delete from doc_attribute where doc_attribute_type_id = ? and value = ? and doc_uuid = ?::uuid";
    private static final String GET_DOC_ATTR_SQL = "select value, last_updated from doc_attribute " +
            "where doc_attribute_type_id = ? and doc_uuid = ?::uuid";
    private static final String GET_DOC_ATTR_HIST_SQL = "select value, last_updated from doc_attribute_hist " +
            "where doc_attribute_type_id = ? and valid_from_ts <= ? and valid_to_ts > ? and doc_uuid = ?::uuid";
    private static final String GET_DOC_ATTRS_SQL = "select name, value from doc_attribute ah join doc_attribute_type t on ah.doc_attribute_type_id = t.doc_attribute_type_id " +
            "where ah.doc_uuid = ?::uuid";
    private static final String GET_DOC_ATTRS_HIST_SQL = "select name, value from doc_attribute_hist ah join doc_attribute_type t on ah.doc_attribute_type_id = t.doc_attribute_type_id " +
            "where ah.valid_from_ts <= ? and ah.valid_to_ts > ? and ah.doc_uuid = ?::uuid";
    private static final String SEARCH_DOC_ATTRS_SQL = "select doc_uuid, name, value, a.last_updated from doc_attribute a join doc_attribute_type t on a.doc_attribute_type_id = t.doc_attribute_type_id where ";
    private static final String SEARCH_DOC_ATTRS_HIST_SQL = "select doc_uuid, name, value, ah.last_updated from doc_attribute_hist ah join doc_attribute_type t on ah.doc_attribute_type_id = t.doc_attribute_type_id where ";
    private static final String GET_ATTR_TYPES_SQL = "select doc_attribute_type_id, name, value_type, description from doc_attribute_type";

    private static final String LOG_DOC_ACCESS = "insert into last_accessed (doc_uuid, client_uuid) values (?::uuid, ?::uuid) on conflict (doc_uuid, client_uuid) do " +
            "update set last_accessed_ts = current_timestamp";

    private static final String SOFT_DELETE_ARCHIVED_SQL = "select f_delete_files('{doc_uuid}', {archive_priority}, {upload_limit})";
    private static final String SOFT_DELETE_SQL = "select f_delete_files('{doc_uuid}')";
    private static final String UN_SOFT_DELETE_SQL = "select f_undelete_files('{doc_uuid}')";
    private static final String GET_SOFT_DELETE_SQL = "select file_location_rel_id, doc_uuid, file_uuid, file_location_uuid from deleted_file_location_rel where doc_uuid = '{doc_uuid}'";
    private static final String PURGE_SQL = "select f_delete_doc('{doc_uuid}')";

    private PreparedStatement insertDocHierSt;
    private PreparedStatement insertDocAttrTypeSt;
    private PreparedStatement getDocAttrTypeIdSt;
    private PreparedStatement insertDocAttrSt;
    private PreparedStatement clearDocAttrSt;
    private PreparedStatement getAttrTypesSt;
    private PreparedStatement logAccessSt;

    private static final Set<String> V_DOC_COLUMNS = new HashSet<String>();
    static
    {
        V_DOC_COLUMNS.add("data_cls");
        V_DOC_COLUMNS.add("data_cls_version");
        V_DOC_COLUMNS.add("env");
        V_DOC_COLUMNS.add("file_pattern");
        V_DOC_COLUMNS.add("doc_uuid");
        V_DOC_COLUMNS.add("source");
        V_DOC_COLUMNS.add("client_uuid");
    }


    private TardisService delegate;


    public TardisServiceDbImpl(Connection connection)
    {
        this.connection = connection;
        this.delegate = this;
    }

    @Override
    public void setDelegate(TardisService delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void indexFile(String fileUUID, String fileLocationUUID, String clientUUID, String docUUID, String hash,
                          DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force, boolean archive)
    {
        String sql;
        try
        {
            HashMap<String, String> fv = new HashMap<String, String>();
            fv.put("doc_uuid", docUUID);
            fv.put("client_uuid", clientUUID);
            fv.put("content", hash);
            fv.put("data_from_ts", TardisUtils.formatDateTime(start));
            fv.put("data_to_ts", TardisUtils.formatDateTime(end));
            fv.put("knowledge_ts", TardisUtils.formatDateTime(knowledgeTime));
            fv.put("file_uuid", fileUUID);
            fv.put("file_meta", fileMeta == null ? "NULL" : "'" + fileMeta + "'");
            fv.put("location_uuid", fileLocationUUID == null ? "NULL" : "'" + fileLocationUUID + "'");
            sql = TardisUtils.formatKvs(force ? INSERT_FILE_SQL : INDEX_FILE_SQL, fv);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        try
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to create: " + sql);
            }
        }
        catch(SQLException e)
        {
            try
            {
                PreparedStatement ps = connection.prepareStatement(sql);
                if(!ps.execute())
                {
                    throw new RuntimeException("failed to create: " + sql);
                }
            }
            catch (SQLException retryException)
            {
                throw new RuntimeException("failed after retry: " + sql, retryException);
            }
        }
    }

    @Override
    public void indexContent(String clientUUID, String docUUID, String content, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force)
    {
        String sql;
        try
        {
            HashMap<String, String> fv = new HashMap<String, String>();
            fv.put("doc_uuid", docUUID);
            fv.put("client_uuid", clientUUID);
            fv.put("content", content);
            fv.put("data_from_ts", TardisUtils.formatDateTime(start));
            fv.put("data_to_ts", TardisUtils.formatDateTime(end));
            fv.put("knowledge_ts", TardisUtils.formatDateTime(knowledgeTime));
            fv.put("file_meta", fileMeta == null ? "NULL" : "'" + fileMeta + "'");
            sql = TardisUtils.formatKvs(force ? INSERT_CONTENT_SQL : INDEX_CONTENT_SQL, fv);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        try
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to create: " + sql);
            }
        }
        catch(SQLException e)
        {
            try
            {
                PreparedStatement ps = connection.prepareStatement(sql);
                if(!ps.execute())
                {
                    throw new RuntimeException("failed to create: " + sql);
                }
            }
            catch (SQLException retryException)
            {
                throw new RuntimeException("failed after retry: " + sql, retryException);
            }
        }
    }

    @Override
    public boolean createDoc(Doc doc)
    {
        String sql = "";
        try
        {
            HashMap<String,String> fv = new HashMap<String,String>();
            fv.put("doc_uuid", doc.getDocUuid());
            fv.put("data_cls", doc.getDataCls());
            fv.put("data_cls_version", doc.getDataClsVersion());
            fv.put("file_pattern", doc.getFilePattern());
            fv.put("env", doc.getEnv());
            fv.put("unique_keys", doc.getUniqueKeys());
            fv.put("client_uuid", doc.getClientUuid());

            sql = TardisUtils.formatKvs(INSERT_DOC_SQL.replace("{source}", doc.getSource()), fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to create doc: " + sql);
            }
            ResultSet rs = ps.getResultSet();
            if (rs.next())
            {
                return rs.getBoolean(1);
            }
            return false;
        }
        catch(SQLException e)
        {
            throw new RuntimeException("failed to create doc: " + sql, e);
        }
    }

    @Override
    public Doc getDoc(String uuid)
    {
        try
        {
            HashMap<String,String> fv = new HashMap<String,String>();
            fv.put("uuid", uuid);
            PreparedStatement ps = connection.prepareStatement(TardisUtils.formatKvs(GET_DOC_BY_UUID_SQL, fv));

            if(!ps.execute())
            {
                throw new RuntimeException();
            }

            if(!ps.getResultSet().next())
            {
                return null;
            }

            //noinspection UnnecessaryLocalVariable
            Doc doc = EntityCrudHandlerFactory.Instance.forType(Doc.class).transform(ps.getResultSet());
            return doc;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getDistinctDocFields(JSONObject query, String field)
    {
        String sql = SEARCH_DISTINCT_FIELDS_SQL.replace("{field}",
                V_DOC_COLUMNS.contains(field) ? field : (
                        field.startsWith("!") ? field.substring(1) : "unique_keys->>'" + field + "'"));
        sql = getSearchSql(query, 0, sql);
        ArrayList<String> values = new ArrayList<String>();
        if (sql == null)
        {
            return values;
        }

        try
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to search with: " + sql);
            }
            ResultSet rs = ps.getResultSet();
            while (rs.next())
            {
                values.add(rs.getString(1));
            }
            return values;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<DocAttribute> searchDocAttributes(JSONObject query, boolean history)
    {
        String sql = history ? SEARCH_DOC_ATTRS_HIST_SQL : SEARCH_DOC_ATTRS_SQL;
        ArrayList<DocAttribute> attrs = new ArrayList<DocAttribute>();
        sql = getSearchSql(query, 0, sql);
        try
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to search with: " + sql);
            }
            ResultSet rs = ps.getResultSet();
            while (rs.next())
            {
                attrs.add(new DocAttributeImpl(rs.getString(1),
                        rs.getString(2), rs.getString(3), rs.getTimestamp(4).getTime()));
            }
            return attrs;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Doc> searchDocs(JSONObject query)
    {
        String sql = getSearchSql(query, 5000, SEARCH_DOC_SQL);
        ArrayList<Doc> docs = new ArrayList<Doc>();
        if (sql == null)
        {
            return docs;
        }

        try
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to search with: " + sql);
            }
            ResultSet rs = ps.getResultSet();
            while (rs.next())
            {
                Doc doc = EntityCrudHandlerFactory.Instance.forType(Doc.class).transform(rs);
                docs.add(doc);
            }
            return docs;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public String getSearchSql(JSONObject query, int limit, String initialSql)
    {
        if (query == null || query.length() == 0)
        {
            return null;
        }

        StringBuilder sb = new StringBuilder(initialSql);
        Iterator<?> keys = query.keys();

        boolean hasQuery = false, first = true;
        String lastQuery = null;
        while (keys.hasNext())
        {
            String key = (String)keys.next();
            String clause = null;
            if (key.equals("__query__"))
            {
                clause = query.getString(key);
                if (clause != null && !clause.isEmpty())
                {
                    lastQuery = clause;
                }
                continue;
            }
            try
            {
                JSONArray values = query.getJSONArray(key);
                int len = values.length();
                if (len > 0)
                {
                    StringBuilder csb = new StringBuilder();
                    csb.append("'").append(values.getString(0)).append("'");
                    for (int i = 1; i < len; i++)
                    {
                        csb.append(",'").append(values.getString(i)).append("'");
                    }
                    clause = " in (" + csb.toString() + ")";
                }
            }
            catch (Exception e)
            {
                clause = query.getString(key);
                if (clause.contains("'"))
                {
                    clause = clause.replaceAll("'", "''");
                }
                if (clause.contains("%"))
                {
                    clause = " like '" + clause + "'";
                }
                else
                {
                    clause = " = '" + clause + "'";
                }
            }
            if (clause != null)
            {
                hasQuery = true;
                if (!first)
                {
                    sb.append(" and ");
                }
                if (V_DOC_COLUMNS.contains(key))
                {
                    sb.append(key);
                }
                else
                {
                    sb.append("unique_keys->>'").append(key).append("'");
                }
                sb.append(clause);
            }
            first = false;
        }
        if (lastQuery != null)
        {
            if (!first)
                sb.append(" and ");
            sb.append(lastQuery);
            hasQuery = true;
        }
        if (!hasQuery)
        {
            return null;
        }

        if (limit > 0)
        {
            sb.append(" limit ").append(limit);
        }
        return sb.toString();

    }

    @Override
    public void createClient(Client client)
    {
        String sql = null;
        String uuid = client.getClientUuid();
        try
        {
            HashMap<String,String> fv = new HashMap<String,String>();
            fv.put("uuid", uuid);
            fv.put("user_name", client.getUserName());
            fv.put("host_name", client.getHostName());
            fv.put("env", client.getEnv());
            fv.put("app_version", client.getAppVersion());
            sql = TardisUtils.formatKvs(INSERT_CLIENT_SQL, fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(ps.execute())
            {
                throw new RuntimeException("failed to create client: " + sql);
            }
        }
        catch(SQLException e)
        {
            if (e.toString().contains("read-only"))
            {
                return;
            }
            throw new RuntimeException("failed to create client: " + sql, e);
        }
    }

    @Override
    public Client getClient(String uuid)
    {
        try
        {
            HashMap<String,String> fv = new HashMap<String,String>();
            fv.put("uuid", uuid);
            PreparedStatement ps = connection.prepareStatement(TardisUtils.formatKvs(GET_CLIENT_BY_UUID_SQL, fv));

            if(!ps.execute())
            {
                throw new RuntimeException();
            }

            if(!ps.getResultSet().next())
            {
                return null;
            }

            //noinspection UnnecessaryLocalVariable
            Client client = EntityCrudHandlerFactory.Instance.forType(Client.class).transform(ps.getResultSet());
            return client;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createLocation(FileLocation fileLocation, boolean force)
    {
        String sql = null;
        try
        {
            HashMap<String,String> fv = new HashMap<String,String>();
            fv.put("uuid", fileLocation.getFileLocationUuid());
            fv.put("server", fileLocation.getServer());
            fv.put("parent_dir", fileLocation.getParentDir());
            fv.put("local_parent_dir", fileLocation.getLocalParentDir());
            fv.put("owner", fileLocation.getOwner());
            fv.put("data_center", fileLocation.getDataCenter());
            fv.put("priority", fileLocation.getPriority() + "");
            sql = TardisUtils.formatKvs(INSERT_FILE_LOCATION_SQL, fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(ps.execute())
            {
                throw new RuntimeException("failed to create location: " + sql);
            }
        }
        catch(SQLException e)
        {
            throw new RuntimeException("failed to create location: " + sql, e);
        }
    }

    @Override
    public String delete(String docUuid, String fileUuid, String fileLocationUuid)
    {
        if (NIL_UUID.equals(fileUuid))
        {
            return null;
        }

        IndexResult ir = getIndex(docUuid, fileUuid);
        if (ir == null || ir.isEmpty() || ir.getLocations().containsKey(fileLocationUuid))
        {
            return null;
        }

        IndexRecord r = ir.getIndexRecords().get(0);
        String fileName = TardisUtils.getFileName(
                delegate.getDoc(docUuid),
                r.getContent(),
                TypeConverterUtils.millisToDateTime(r.getDataFromTs()),
                TypeConverterUtils.millisToDateTime(r.getDataToTs()));
        try
        {
            HashMap<String, String> fv = new HashMap<String, String>();
            fv.put("doc_uuid", docUuid);
            fv.put("file_uuid", fileUuid);
            fv.put("location_uuid", fileLocationUuid);
            String sql = TardisUtils.formatKvs(DELETE_FILE_SQL, fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if (ps.execute())
            {
                throw new RuntimeException("failed to delete with sql: " + sql);
            }

            sql = TardisUtils.formatKvs(CONFLICT_FILES_SQL, fv);
            ps = connection.prepareStatement(sql);
            if (!ps.execute())
            {
                throw new RuntimeException("failed to get conflict files: " + sql);
            }
            ResultSet rs = ps.getResultSet();
            while (rs.next())
            {
                Doc conflictDoc = delegate.getDoc(rs.getString(1));
                String conflictFileName = TardisUtils.getFileName(
                        conflictDoc,
                        rs.getString(2),
                        TypeConverterUtils.millisToDateTime(rs.getTimestamp(3).getTime()),
                        TypeConverterUtils.millisToDateTime(rs.getTimestamp(4).getTime()));
                if (fileName.equals(conflictFileName))
                {
                    return null;
                }
            }

            return fileName;
        }
        catch (Exception e)
        {
            throw new RuntimeException("failed to get delete " + docUuid + "/" + fileUuid + "/" + fileLocationUuid + ": " + e);
        }
    }

    @Override
    public boolean softDeleteFiles(String docUuid, int archivePriority, int uploadLimit)
    {
        try
        {
            HashMap<String, String> fv = new HashMap<String, String>();
            fv.put("doc_uuid", docUuid);
            fv.put("archive_priority", Integer.toString(archivePriority));
            fv.put("upload_limit", Integer.toString(uploadLimit));
            String sql = TardisUtils.formatKvs(SOFT_DELETE_ARCHIVED_SQL, fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException();
            }
            ResultSet rs = ps.getResultSet();
            if(rs.next())
            {
                return rs.getString(1) != null;
            }
            return false;
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean softDeleteFiles(String docUuid, String clientUuid)
    {
        try
        {
            PreparedStatement ps = connection.prepareStatement(SOFT_DELETE_SQL.replace("{doc_uuid}", docUuid));
            if(!ps.execute())
            {
                throw new RuntimeException();
            }
            DocAttributeType dat = delegate.getDocAttributeType("_deleted");
            setDocAttribute(
                    docUuid,
                    dat.getName(),
                    TardisUtils.formatDateTime(new DateTime(DateTimeZone.UTC)),
                    clientUuid);
            ResultSet rs = ps.getResultSet();
            if(rs.next())
            {
                return rs.getString(1) != null;
            }
            return false;
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean unSoftDeleteFiles(String docUuid, String clientUuid)
    {
        try
        {
            PreparedStatement ps = connection.prepareStatement(UN_SOFT_DELETE_SQL.replace("{doc_uuid}", docUuid));
            if(!ps.execute())
            {
                throw new RuntimeException();
            }
            DocAttributeType dat = delegate.getDocAttributeType("_deleted");
            clearDocAttribute(docUuid, dat.getName(), clientUuid);
            ResultSet rs = ps.getResultSet();
            if(rs.next())
            {
                return rs.getString(1) != null;
            }
            return false;
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<FileLocationRel> getSoftDeletedFiles(String docUuid)
    {
        try
        {
            PreparedStatement ps = connection.prepareStatement(GET_SOFT_DELETE_SQL.replace("{doc_uuid}", docUuid));
            if(!ps.execute())
            {
                throw new RuntimeException();
            }
            ResultSet rs = ps.getResultSet();
            List<FileLocationRel> rels = new ArrayList<FileLocationRel>();
            while (rs.next())
            {
                rels.add(new FileLocationRelImpl(rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rels;
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean purgeDoc(String docUuid)
    {
        try
        {
            PreparedStatement ps = connection.prepareStatement(PURGE_SQL.replace("{doc_uuid}", docUuid));
            if(!ps.execute())
            {
                throw new RuntimeException();
            }
            ResultSet rs = ps.getResultSet();
            if(rs.next())
            {
                return rs.getBoolean(1);
            }
            return false;
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<UploadLocation> getUploadLocations()
    {
        try
        {
            PreparedStatement ps = connection.prepareStatement(GET_UPLOAD_LOCATIONS_SQL);
            if(!ps.execute())
            {
                throw new RuntimeException();
            }
            ResultSet rs = ps.getResultSet();
            ArrayList<UploadLocation> services = new ArrayList<UploadLocation>();
            EntityCrudHandler<FileLocation> locationHandler = EntityCrudHandlerFactory.Instance.forType(FileLocation.class);
            while (rs.next())
            {
                services.add(new UploadLocationImpl(locationHandler.transform(rs),
                        rs.getString("upload_url"), rs.getString("sources"),
                        rs.getBoolean("is_active")));
            }
            return services;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setDocAttribute(String docUuid, String name, String value, String clientUuid)
    {
        if (value == null)
        {
            throw new RuntimeException("cannot set " + docUuid + " attribute " + name + " to null");
        }

        DocAttributeType type = delegate.getDocAttributeType(name);
        if (type == null)
        {
            throw new RuntimeException("attribute type " + name + " does not exist");
        }
        value = type.cleanValue(value);

        try
        {
            if (insertDocAttrSt == null)
            {
                insertDocAttrSt = connection.prepareStatement(INSERT_DOC_ATTR_SQL);
            }
            insertDocAttrSt.setString(1, docUuid);
            insertDocAttrSt.setString(2, docUuid);
            insertDocAttrSt.setInt(3, type.getDocAttributeTypeId());
            insertDocAttrSt.setString(4, value);
            insertDocAttrSt.setString(5, clientUuid);
            if (insertDocAttrSt.execute())
            {
                throw new RuntimeException("failed to insert doc attribute: " + docUuid + "(" + name + "): " + value);
            }
            return insertDocAttrSt.getUpdateCount() != 0;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean clearDocAttribute(String docUuid, String name, String clientUuid)
    {
        setDocAttribute(docUuid, name, TardisUtils.DELETED_ATTRIBUTE_VALUE, clientUuid);
        DocAttributeType type = delegate.getDocAttributeType(name);
        try
        {
            if (clearDocAttrSt == null)
            {
                clearDocAttrSt = connection.prepareStatement(DELETE_DOC_ATTR_SQL);
            }
            clearDocAttrSt.setInt(1, type.getDocAttributeTypeId());
            clearDocAttrSt.setString(2, TardisUtils.DELETED_ATTRIBUTE_VALUE);
            clearDocAttrSt.setString(3, docUuid);
            if (clearDocAttrSt.execute())
            {
                throw new RuntimeException("failed to clear doc attribute: " + docUuid + "(" + name + ")");
            }
            return clearDocAttrSt.getUpdateCount() != 0;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocAttribute getDocAttribute(String docUuid, String name, DateTime knowledge)
    {
        DocAttributeType type = delegate.getDocAttributeType(name);
        if (type == null)
        {
            throw new RuntimeException("attribute type " + name + " does not exist");
        }
        try
        {
            PreparedStatement ps;
            if (knowledge == null || !knowledge.isBefore(TardisUtils.DEFAULT_END_TS))
            {
                ps = connection.prepareStatement(GET_DOC_ATTR_SQL);
                ps.setInt(1, type.getDocAttributeTypeId());
                ps.setString(2, docUuid);
            }
            else
            {
                ps = connection.prepareStatement(GET_DOC_ATTR_HIST_SQL);
                ps.setInt(1, type.getDocAttributeTypeId());
                ps.setTimestamp(2, TypeConverterUtils.toTimestamp(knowledge));
                ps.setTimestamp(3, TypeConverterUtils.toTimestamp(knowledge));
                ps.setString(4, docUuid);
            }
            if (!ps.execute())
            {
                throw new RuntimeException("failed to get doc attribute: " + docUuid + "(" + name + ")");
            }
            ResultSet rs = ps.getResultSet();
            if (!rs.next())
            {
                return new DocAttributeImpl(docUuid, name, null, 0);
            }
            return new DocAttributeImpl(docUuid, name, rs.getString(1), rs.getTimestamp(2).getTime());
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getDocAttributes(String docUuid, DateTime knowledge)
    {
        try
        {
            PreparedStatement ps;
            if (knowledge == null || !knowledge.isBefore(TardisUtils.DEFAULT_END_TS))
            {
                ps = connection.prepareStatement(GET_DOC_ATTRS_SQL);
                ps.setString(1, docUuid);
            }
            else
            {
                ps = connection.prepareStatement(GET_DOC_ATTRS_HIST_SQL);
                ps.setTimestamp(1, TypeConverterUtils.toTimestamp(knowledge));
                ps.setTimestamp(2, TypeConverterUtils.toTimestamp(knowledge));
                ps.setString(3, docUuid);
            }
            if (!ps.execute())
            {
                throw new RuntimeException("failed to get doc attributes: " + docUuid);
            }
            ResultSet rs = ps.getResultSet();
            Map<String, String> map = new HashMap<String, String>();
            while (rs.next())
            {
                map.put(rs.getString(1), rs.getString(2));
            }
            return map;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<DocAttributeType> getAttributeTypes()
    {
        try
        {
            if (getAttrTypesSt == null)
            {
                getAttrTypesSt = connection.prepareStatement(GET_ATTR_TYPES_SQL);
            }
            if (!getAttrTypesSt.execute())
            {
                throw new RuntimeException("failed to get attribute types: " + GET_ATTR_TYPES_SQL);
            }
            ResultSet rs = getAttrTypesSt.getResultSet();
            List<DocAttributeType> types = new ArrayList<DocAttributeType>();
            while (rs.next())
            {
                types.add(new DocAttributeTypeImpl(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return types;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
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
    public void addUploadLocation(String fileLocationUuid, String uploadUrl, String permittedSources)
    {
        FileLocation location = delegate.getLocation(fileLocationUuid, false);
        if (DataCenter.valueOf(location.getDataCenter()) == DataCenter.NONE)
        {
            return;
        }

        String sql;
        try
        {
            HashMap<String, String> fv = new HashMap<String, String>();
            fv.put("location_uuid", fileLocationUuid);
            fv.put("upload_url", uploadUrl);
            fv.put("sources", (permittedSources == null || permittedSources.isEmpty()) ? "NULL" : "'" + permittedSources + "'");
            sql = TardisUtils.formatKvs(INSERT_UPLOAD_LOCATION_SQL, fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed add upload service: " + sql);
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeUploadLocation(String fileLocationUuid)
    {
        String sql;
        try
        {
            sql = DELETE_UPLOAD_LOCATION_SQL.replace("{location_uuid}", fileLocationUuid);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(ps.execute())
            {
                throw new RuntimeException("failed add upload service: " + sql);
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDocHierarchy(List<DocHierarchy> docHierarchies)
    {
        if (docHierarchies == null)
        {
            return;
        }
        Map<String, DocHierarchy> hashes = new HashMap<String, DocHierarchy>();
        Set<String> parentDocUuids = new HashSet<String>();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (DocHierarchy dh : docHierarchies)
        {
            hashes.put(DocHierarchyImpl.getUniqueKey(dh), dh);
            if (parentDocUuids.contains(dh.getParentDocUuid()))
            {
                continue;
            }
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(",");
            }
            sb.append("'").append(dh.getParentDocUuid()).append("'");
            parentDocUuids.add(dh.getParentDocUuid());
        }
        String sql = GET_DOC_HIER_FOR_UPDATE_SQL.replace("{doc_uuids}", sb.toString());
        Integer transactionIsolation = null;
        try
        {
            connection.setAutoCommit(false);
            transactionIsolation = connection.getTransactionIsolation();
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            PreparedStatement ps = connection.prepareStatement(sql);
            if (!ps.execute())
            {
                throw new RuntimeException("failed executing " + sql);
            }
            ResultSet rs = ps.getResultSet();
            List<Integer> toDelete = new ArrayList<Integer>();
            String hash;
            while (rs.next())
            {
                hash = DocHierarchyImpl.getUniqueKey(new DocHierarchyImpl(
                        rs.getString(1), // parent_doc_uuid
                        rs.getString(2), // doc_uuid,
                        TypeConverterUtils.millisToDateTime(rs.getTimestamp(3).getTime()),
                        TypeConverterUtils.millisToDateTime(rs.getTimestamp(4).getTime()),
                        rs.getInt(5),
                        rs.getString(6)));
                if (hashes.containsKey(hash))
                {
                    hashes.remove(hash);
                }
                else
                {
                    toDelete.add(rs.getInt(7));
                }
            }
            if (toDelete.size() > 0)
            {
                sb = new StringBuilder();
                first = true;
                for (int id : toDelete)
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        sb.append(",");
                    }
                    sb.append(id);
                }
                sql = DELETE_DOC_HIER_BY_IDS_SQL.replace("{ids}", sb.toString());
                ps = connection.prepareStatement(sql);
                if (ps.execute())
                {
                    throw new RuntimeException("failed to execute " + sql);
                }
            }
            if (insertDocHierSt == null)
            {
                insertDocHierSt = connection.prepareStatement(INSERT_DOC_HIER_SQL);
            }
            for (DocHierarchy dh : hashes.values())
            {
                insertDocHierSt.setString(1, dh.getParentDocUuid());
                insertDocHierSt.setString(2, dh.getDocUuid());
                insertDocHierSt.setTimestamp(3, TypeConverterUtils.toTimestamp(dh.getDataFromTs()));
                insertDocHierSt.setTimestamp(4, TypeConverterUtils.toTimestamp(dh.getDataToTs()));
                insertDocHierSt.setInt(5, dh.getHierOrder());
                insertDocHierSt.setString(6, dh.getClientUuid());
                if (!insertDocHierSt.execute())
                {
                    throw new RuntimeException("failed to insert " + DocHierarchyImpl.getUniqueKey(dh));
                }
            }
            connection.commit();
        }
        catch (Exception e)
        {
            try
            {
                connection.rollback();
            }
            catch (SQLException ignore)
            {
            }
            throw new RuntimeException(e);
        }
        finally
        {
            try
            {
                if (transactionIsolation != null)
                    connection.setTransactionIsolation(transactionIsolation);
                connection.setAutoCommit(true);
            }
            catch (SQLException ignore)
            {
            }
        }
    }

    @Override
    public DocHierarchyResult getDocHierarchy(String docUuid, DateTime knowledge)
    {
        String sql;
        final Set<String> docUuids = new HashSet<String>();
        final DocHierarchyResult dhr = new DocHierarchyResult();
        try
        {
            HashMap<String, String> fv = new HashMap<String, String>();
            fv.put("doc_uuid", docUuid);
            fv.put("knowledge", TardisUtils.formatDateTime(knowledge));
            sql = TardisUtils.formatKvs(GET_DOC_HIER_SQL, fv);
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!ps.execute())
            {
                throw new RuntimeException("failed to execute: " + sql);
            }
            DocHierarchy dh;
            ResultSet rs = ps.getResultSet();
            while (rs.next())
            {
                dh = new DocHierarchyImpl(
                        rs.getString(1), // parent_doc_uuid
                        rs.getString(2), // doc_uuid,
                        TypeConverterUtils.millisToDateTime(rs.getTimestamp(3).getTime()),
                        TypeConverterUtils.millisToDateTime(rs.getTimestamp(4).getTime()),
                        rs.getInt(5),
                        rs.getString(6));
                dhr.addDocHierarchy(dh);
                docUuids.add(dh.getDocUuid());
                docUuids.add(dh.getParentDocUuid());
            }
            for (String uuid: docUuids)
            {
                dhr.addDoc(delegate.getDoc(uuid));
            }
            return dhr;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDocAttributeType(String name, String valueType, String description, String clientUuid)
    {
        DocAttributeType type = delegate.getDocAttributeType(name);
        if (type != null)
        {
            if (!type.getValueType().equals(valueType))
                throw new RuntimeException("name " + name + " has value type " + type.getValueType() + " instead of " + valueType);
            return;
        }

        try
        {
            DocAttributeType.ValueType.valueOf(valueType);
            if (insertDocAttrTypeSt == null)
            {
                insertDocAttrTypeSt = connection.prepareStatement(INSERT_DOC_ATTR_TYPE_SQL);
            }
            insertDocAttrTypeSt.setString(1, name);
            insertDocAttrTypeSt.setString(2, valueType);
            insertDocAttrTypeSt.setString(3, description);
            insertDocAttrTypeSt.setString(4, clientUuid);
            if (insertDocAttrTypeSt.execute())
            {
                throw new RuntimeException("failed to insert doc attribute type: " + name + "(" + valueType + "): " + description);
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }

        type = delegate.getDocAttributeType(name);
        if (!valueType.equals(type.getValueType()))
        {
            throw new RuntimeException("name " + name + " has value type " + type.getValueType() + " instead of " + valueType);
        }
    }

    @Override
    public DocAttributeType getDocAttributeType(String name)
    {
        if (name == null || name.isEmpty())
        {
            throw new RuntimeException("name cannot be null");
        }
        try
        {
            if (getDocAttrTypeIdSt == null)
            {
                getDocAttrTypeIdSt = connection.prepareStatement(GET_DOC_ATTR_TYPE_ID_SQL);
            }
            getDocAttrTypeIdSt.setString(1, name);
            if (!getDocAttrTypeIdSt.execute())
            {
                throw new RuntimeException("failed to get doc attribute type: " + name);
            }
            ResultSet rs = getDocAttrTypeIdSt.getResultSet();
            if (!rs.next())
            {
                return null;
            }
            return new DocAttributeTypeImpl(rs.getInt(1), rs.getString(2), rs.getString(3));
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileLocation getLocation(String uuid, boolean force)
    {
        try
        {
            HashMap<String,String> fv = new HashMap<String, String>();
            fv.put("uuid", uuid);
            PreparedStatement ps = connection.prepareStatement(TardisUtils.formatKvs(GET_FILE_LOCATION_BY_UUID_SQL, fv));

            if(!ps.execute())
            {
                throw new RuntimeException();
            }

            if(!ps.getResultSet().next())
            {
                return null;
            }

            //noinspection UnnecessaryLocalVariable
            FileLocation loc = EntityCrudHandlerFactory.Instance.forType(FileLocation.class).transform(ps.getResultSet());
            return loc;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexResult getIndex(String docUuid, DateTime start, DateTime end, DateTime knowledge, Method method)
    {
        final String sql = getIndexSql(method, docUuid, start, end, knowledge);
        return getIndexSql(sql);
    }

    @Override
    public IndexResult getIndex(String docUUID, String fileUUID)
    {
        final String sql = FILE_SQL.replace("{doc_uuid}", docUUID).replace("{file_uuid}", fileUUID);
        return getIndexSql(sql);
    }

    @Override
    public IndexResult getIndex(String docUUID)
    {
        final String sql = ALL_FILE_SQL.replace("{doc_uuid}", docUUID);
        return getIndexSql(sql);
    }

    private IndexResult getIndexSql(String sql)
    {
        final IndexResult irc = new IndexResult();
        final Set<String> docUuids = new HashSet<String>();
        final Set<String> locationUuids = new HashSet<String>();

        try
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            if (!ps.execute())
            {
                throw new RuntimeException("expected rows from " + sql);
            }
            ResultSet rs = ps.getResultSet();

            IndexRecord ir;
            String fileUuid;
            while (rs.next())
            {
                fileUuid = rs.getString(7);
                if (fileUuid == null)
                {
                    ir = new IndexRecordImpl(
                            rs.getString(1),  // docUuid
                            rs.getInt(2),     // hierOrder
                            rs.getString(3),  // parentDocUuid
                            rs.getInt(4),     // parentHierOrder
                            // doc valid range start/end
                            rs.getTimestamp(5).getTime(),
                            rs.getTimestamp(6).getTime(),
                            null, 0, 0, null, 0, 0, null, null
                    );
                }
                else
                {
                    ir = new IndexRecordImpl(
                            rs.getString(1),  // docUuid
                            rs.getInt(2),     // hierOrder
                            rs.getString(3),  // parentDocUuid
                            rs.getInt(4),     // parentHierOrder
                            // doc valid range start/end
                            rs.getTimestamp(5).getTime(),
                            rs.getTimestamp(6).getTime(),
                            fileUuid,
                            // file valid knowledge start/end
                            rs.getTimestamp(8).getTime(),
                            rs.getTimestamp(9).getTime(),
                            rs.getString(10), // content
                            // file valid data start/end
                            rs.getTimestamp(11).getTime(),
                            rs.getTimestamp(12).getTime(),
                            rs.getString(13), // file meta
                            rs.getString(14)  // file location uuids
                    );
                    if (!fileUuid.equals(NIL_UUID) && ir.getLocationUuids() != null)
                    {
                        Collections.addAll(locationUuids, ir.splitLocationUuids());
                    }
                }
                docUuids.add(ir.getDocUuid());
                irc.add(ir);
            }

            for (String uuid : docUuids)
            {
                irc.addDoc(delegate.getDoc(uuid));
            }
            for (String uuid : locationUuids)
            {
                irc.addLocation(delegate.getLocation(uuid, false));
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
        return irc;
    }

    private String getIndexSql(Method method, String docUuid, DateTime start, DateTime end, DateTime knowledge)
    {
        String view;
        if (method == Method.LOOK_AHEAD)
        {
            view = LOOK_AHEAD_SQL;
        }
        else if (method == Method.LOOK_BEHIND)
        {
            view = LOOK_BEHIND_SQL;
        }
        else
        {
            throw new RuntimeException("unsupported method: " + method);
        }
        HashMap<String,String> fv = new HashMap<String, String>();
        fv.put("doc_uuid", docUuid);
        fv.put("start_ts", TardisUtils.formatDateTime(start));
        fv.put("end_ts", TardisUtils.formatDateTime(end));
        fv.put("knowledge_ts", TardisUtils.formatDateTime(knowledge));
        return TardisUtils.formatKvs(view, fv);
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

    @Override
    public SearchResult lookAhead(String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        return query(LOOK_AHEAD_SQL, docUUID, start, end, knowledge);
    }

    @Override
    public SearchResult lookBehind(String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        return query(LOOK_BEHIND_SQL, docUUID, start, end, knowledge);
    }

    private SearchResult query(String view, String docUUID, DateTime start, DateTime end, DateTime knowledge)
    {
        try
        {
            HashMap<String,String> fv = new HashMap<String, String>();
            fv.put("doc_uuid", docUUID);
            fv.put("start_ts", TardisUtils.formatDateTime(start));
            fv.put("end_ts", TardisUtils.formatDateTime(end));
            fv.put("knowledge_ts", TardisUtils.formatDateTime(knowledge));
            final String sql = TardisUtils.formatKvs(view, fv);
            PreparedStatement ps = connection.prepareStatement(sql);

            if(!ps.execute())
            {
                throw new RuntimeException();
            }

            ResultSet rs = ps.getResultSet();
            SearchResult sr = new SearchResult();

            while (rs.next())
            {
                // TypeConverterUtils.sqlTimestampToMicros(rs.getTimestamp(5));
                sr.add(this,
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        rs.getString(6),
                        rs.getString(7),
                        rs.getString(8),
                        rs.getString(9),
                        rs.getString(10),
                        rs.getString(11),
                        rs.getString(12),
                        rs.getString(13),
                        rs.getString(14));
            }

            return sr;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dispose()
    {
        try
        {
            if (connection != null)
            {
                connection.close();
            }
        }
        catch (SQLException e)
        {
             throw new RuntimeException(e);
        }
    }
    @Override
    public void logAccess(String docUuid, String clientUuid)
    {
        try
        {
            if (logAccessSt == null)
            {
                logAccessSt = connection.prepareStatement(LOG_DOC_ACCESS);
            }
            logAccessSt.setString(1, docUuid);
            logAccessSt.setString(2, clientUuid);
            if (logAccessSt.execute())
            {
                throw new RuntimeException("failed to log access of doc " + docUuid + " by " + clientUuid);
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
