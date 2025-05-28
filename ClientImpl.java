package com.teza.common.tardis;

import com.teza.common.datasvcs.client.ClientContextImpl;
import com.teza.common.env.TezaEnvFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * User: robin
 * Date: 9/21/16
 * Time: 1:50 PM
 */
public class ClientImpl implements Client
{
    private static final Client instance = new ClientImpl();
    private String clientUuid, userName, hostName, env, appVersion;

    public static Client getInstance()
    {
        return instance;
    }

    public ClientImpl()
    {
        userName = System.getProperty("user.name", "");
        try
        {
            hostName = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e)
        {
            hostName = "";
        }
        String[] parts = TezaEnvFactory.getTezaEnv().getEnvName().split("/", 0);
        env = parts[parts.length - 1];
        appVersion = new ClientContextImpl(userName).getApplication();
        if (appVersion.startsWith("tmp") && appVersion.endsWith("-instance"))
        {
            appVersion = "tmpGRID-instance";
        }
        clientUuid = TardisUtils.getClientUuid(userName, hostName, env, appVersion);
    }

    public ClientImpl(String clientUuid, String userName, String hostName, String env, String appVersion)
    {
        if (clientUuid == null || userName == null || hostName == null || env == null || appVersion == null)
        {
            throw new RuntimeException("cannot specify null values for any input");
        }
        if (clientUuid.isEmpty() || userName.isEmpty() || hostName.isEmpty() || env.isEmpty() || appVersion.isEmpty())
        {
            throw new RuntimeException("cannot specify empty values for any input");
        }
        this.clientUuid = clientUuid;
        this.userName = userName;
        this.hostName = hostName;
        this.env = env;
        this.appVersion = appVersion;
    }

    public String getClientUuid()
    {
        return clientUuid;
    }

    public String getUserName()
    {
        return userName;
    }

    public String getHostName()
    {
        return hostName;
    }

    public String getEnv()
    {
        return env;
    }

    public String getAppVersion()
    {
        return appVersion;
    }

    public void setClientUuid(String value) { clientUuid = value; }
    public void setUserName(String value) { userName = value; }
    public void setHostName(String value) { hostName = value; }
    public void setEnv(String value) { env = value; }
    public void setAppVersion(String value) { appVersion = value; }
}
