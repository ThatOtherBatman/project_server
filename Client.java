package com.teza.common.tardis;

/**
 * User: robin
 * Date: 9/21/16
 * Time: 1:50 PM
 */

/**
 * tardis keeps track of who indexed data into tardis, which is represented by
 * an instance of this interface
 */
public interface Client
{
    String getClientUuid();
    String getUserName();
    String getHostName();
    String getEnv();
    String getAppVersion();

    void setClientUuid(String value);
    void setUserName(String value);
    void setHostName(String value);
    void setEnv(String value);
    void setAppVersion(String value);
}
