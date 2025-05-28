package com.teza.common.tardis;

/**
 * User: tom
 * Date: 1/13/17
 * Time: 1:52 PM
 */
public class FileLocationImpl implements FileLocation
{
    private String fileLocationUuid, server, parentDir, localParentDir, owner, dataCenter;
    private int priority;

    public FileLocationImpl(String server, String parentDir, String localParentDir, String owner,
                            String dataCenter, int priority)
    {

        this(TardisUtils.getFileLocationUuid(server, parentDir), server, parentDir, localParentDir, owner, dataCenter, priority);
    }

    public FileLocationImpl(String fileLocationUuid, String server, String parentDir, String localParentDir, String owner,
                            String dataCenter, int priority)
    {
        this.fileLocationUuid = fileLocationUuid;
        this.server = server;
        this.parentDir = parentDir;
        this.localParentDir = localParentDir;
        this.owner = owner;
        this.dataCenter = dataCenter;
        this.priority = priority;
    }

    @Override
    public String getFileLocationUuid()
    {
        return fileLocationUuid;
    }

    @Override
    public String getServer()
    {
        return server;
    }

    @Override
    public String getParentDir()
    {
        return parentDir;
    }

    @Override
    public String getLocalParentDir()
    {
        return localParentDir;
    }

    @Override
    public String getOwner()
    {
        return owner;
    }

    @Override
    public String getDataCenter()
    {
        return dataCenter;
    }

    @Override
    public int getPriority()
    {
        return priority;
    }

    @Override
    public void setFileLocationUuid(String v)
    {
        fileLocationUuid = v;
    }

    @Override
    public void setServer(String v)
    {
        server = v;
    }

    @Override
    public void setParentDir(String v)
    {
        parentDir = v;
    }

    @Override
    public void setLocalParentDir(String v)
    {
        localParentDir = v;
    }

    @Override
    public void setOwner(String v)
    {
        owner = v;
    }

    @Override
    public void setDataCenter(String v)
    {
        dataCenter = v;
    }

    @Override
    public void setPriority(int v)
    {
        priority = v;
    }
}
