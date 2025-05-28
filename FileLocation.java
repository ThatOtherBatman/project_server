package com.teza.common.tardis;

/**
 * User: robin
 * Date: 9/19/16
 * Time: 2:15 PM
 */

/**
 * file location where data are stored in Tardis
 */
public interface FileLocation
{
    String getFileLocationUuid();
    String getServer();
    String getParentDir();
    String getLocalParentDir();
    String getOwner();
    String getDataCenter();
    int getPriority();

    // required for EntityCrudHandler:
    void setFileLocationUuid(String v);
    void setServer(String v);
    void setParentDir(String v);
    void setLocalParentDir(String v);
    void setOwner(String v);
    void setDataCenter(String v);
    void setPriority(int v);
}