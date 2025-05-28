package com.teza.common.tardis;

import org.json.JSONObject;

/**
 * User: tom
 * Date: 10/7/16
 * Time: 1:50 PM
 */
public class DocImpl implements Doc
{
    protected static final String defaultDataCls = "teza.common.tardis.datatype.dataframe:LegacyColumnSetFile";
    protected static final String defaultDataClsVersion = "0";

    protected String source, docUuid, dataCls, dataClsVersion, filePattern, env, uniqueKeys, clientUuid;

    public DocImpl(Source source, JSONObject uniqueKeys, String filePattern, String permissionGroup)
    {
        this(source, uniqueKeys, filePattern, permissionGroup, defaultDataCls, defaultDataClsVersion);
    }

    public DocImpl(Source source, JSONObject uniqueKeys, String filePattern, String permissionGroup,
                   String dataCls, String dataClsVersion)
    {
        Client client = ClientImpl.getInstance();
        env = (permissionGroup == null || permissionGroup.isEmpty()) ? client.getEnv() : permissionGroup;
        this.source = source.getShortName();
        this.dataCls = dataCls;
        this.dataClsVersion = dataClsVersion;
        this.filePattern = filePattern;
        this.uniqueKeys = uniqueKeys.toString();
        this.clientUuid = client.getClientUuid();
        docUuid = TardisUtils.getDocUuid(source, dataCls, dataClsVersion, filePattern, env, uniqueKeys);
    }

    public DocImpl(String source, String docUuid, String dataCls, String dataClsVersion,
                   String filePattern, String env, String uniqueKeys, String clientUuid)
    {
        if (source == null || docUuid == null || dataCls == null || dataClsVersion == null || filePattern == null || env == null || uniqueKeys == null || clientUuid == null)
        {
            throw new RuntimeException("cannot specify null values for any input");
        }
        if (source.isEmpty() || docUuid.isEmpty() || dataCls.isEmpty() || dataClsVersion.isEmpty() || filePattern.isEmpty() || env.isEmpty() || uniqueKeys.isEmpty() || clientUuid.isEmpty())
        {
            throw new RuntimeException("cannot specify empty values for any input");
        }
        this.source = source;
        this.docUuid = docUuid;
        this.dataCls = dataCls;
        this.dataClsVersion = dataClsVersion;
        this.filePattern = filePattern;
        this.env = env;
        this.uniqueKeys = uniqueKeys;
        this.clientUuid = clientUuid;
    }

    public String getSource() { return source; }
    public String getDocUuid() { return docUuid; }
    public String getDataCls() { return dataCls; }
    public String getDataClsVersion() {return dataClsVersion; }
    public String getFilePattern() { return filePattern; }
    public String getEnv() { return env; }
    public String getUniqueKeys() { return uniqueKeys; }
    public String getClientUuid() { return clientUuid; }

    public void setSource(String value) { source = value; }
    public void setDocUuid(String value) { docUuid = value; }
    public void setDataCls(String value) { dataCls = value; }
    public void setDataClsVersion(String value) { dataClsVersion = value; }
    public void setFilePattern(String value) { filePattern = value; }
    public void setEnv(String value) { env = value; }
    public void setUniqueKeys(String value) { uniqueKeys = value; }
    public void setClientUuid(String value) { clientUuid = value; }
}
