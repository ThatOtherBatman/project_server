package com.teza.common.tardis;

import com.teza.common.env.TezaEnv;
import com.teza.common.env.TezaEnvFactory;
import com.teza.common.util.PropertyWrapper;

import java.io.IOException;
import java.net.URL;

/**
 * User: tom
 * Date: 2/21/18
 * Time: 11:51 AM
 */
public class TardisEnv
{
    public static final String PROP_TARDIS_ENV = "teza.tardis.env";
    public static final String PROP_TARDIS_ENV_PATH = "teza.tardis.envpath";
    public static final String SOURCE_ENVIRONMENT_KEY = "SOURCE_ENVIRONMENT";

    private final String tardisEnvPath, env;

    public TardisEnv()
    {
        this(null);
    }

    public TardisEnv(String env)
    {
        if (env == null || env.isEmpty())
        {
            String[] parts = TezaEnvFactory.getTezaEnv().getEnvName().split("/", 0);
            env = parts[parts.length - 1];
            env = System.getProperty(PROP_TARDIS_ENV, env);
        }
        this.env = env;

        String envPath = System.getProperty(TezaEnv.PROP_TEZA_ENV_PATH);
        envPath = System.getProperty( PROP_TARDIS_ENV_PATH, envPath );

        if ( envPath == null ) envPath = "http://runbits.teza.com/catapult/cfg/teza_env";
        if( ! envPath.endsWith( "/" ) ) envPath += "/";

        this.tardisEnvPath = envPath;
    }

    public String getConfigUrl()
    {
        return tardisEnvPath + env + "/" + "uploadsvc/tardis-common.ini";
    }

    public PropertyWrapper load()
    {
        String url = getConfigUrl();
        PropertyWrapper pw = new PropertyWrapper();
        try
        {
            pw.loadURL(new URL(url));
        }
        catch (IOException e)
        {
            throw new RuntimeException("could not load " + url);
        }

        if (!pw.containsKey(SOURCE_ENVIRONMENT_KEY)) pw.setProperty(SOURCE_ENVIRONMENT_KEY, env);
        return pw;
    }
}
