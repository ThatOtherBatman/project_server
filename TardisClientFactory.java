package com.teza.common.tardis;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.teza.common.env.TezaEnvFactory;
import com.teza.common.tardis.caches.TenureCache;
import com.teza.common.util.PropertyWrapper;

import javax.ws.rs.core.Cookie;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * User: tom
 * Date: 1/9/17
 * Time: 3:34 PM
 */
public abstract class TardisClientFactory
{

    public final static String PROP_TARDIS_ENV = "teza.tardis.env";
    public static final Cache<String, PropertyWrapper> envs = CacheBuilder.newBuilder().maximumSize(4).expireAfterWrite(1, TimeUnit.HOURS).build();
    public static final int DEV = 0;
    public static final int LOCAL = 1;
    public static final int PROD = 2;
    public static final int RD = 3;

    public static TardisClient getTardisClient()
    {
        return getTardisClient(TardisWebClient.MAX_TIMEOUT, TardisWebClient.MAX_TIMEOUT, TardisWebClient.RETRY_TIMEOUT);
    }

    public static int getModeFromUri(String uri)
    {
        uri = uri.replace("tardis://", "");
        String[] parts = uri.split("/", -1);
        if (parts[0].equals("prod"))
        {
            return PROD;
        }
        else if (parts[0].equals("dev"))
        {
            return DEV;
        }
        else if (parts[0].equals("research"))
        {
            return RD;
        }
        throw new IllegalArgumentException("invalid tardis uri: " + uri);
    }

    public static TardisClient getTardisClient(String tardisUri)
    {
        return getTardisClient(getModeFromUri(tardisUri));
    }

    public static TardisClient getTardisClient(int connectTimeoutInMillis, int readTimeoutInMillis, int retryTimeoutInMillis)
    {
        String[] parts = TezaEnvFactory.getTezaEnv().getEnvName().split("/", 0);
        String env = parts[parts.length - 1];
        env = System.getProperty(PROP_TARDIS_ENV, env);
        if (env.equals("RESEARCH_DEFAULT"))
        {
            return getTardisClient(RD, connectTimeoutInMillis, readTimeoutInMillis, retryTimeoutInMillis);
        }
        else if (env.equals("DEV"))
        {
            return getTardisClient(DEV, connectTimeoutInMillis, readTimeoutInMillis, retryTimeoutInMillis);
        }
        else
        {
            return getTardisClient(PROD, connectTimeoutInMillis, readTimeoutInMillis, retryTimeoutInMillis);
        }
    }

    public static TardisClient getTardisClient(int mode)
    {
        return getTardisClient(mode, TardisWebClient.MAX_TIMEOUT, TardisWebClient.MAX_TIMEOUT, TardisWebClient.RETRY_TIMEOUT);
    }

    public static TardisClient getTardisClient(int mode, int connectTimeoutInMillis, int readTimeoutInMillis, int retryTimeoutInMillis)
    {
        boolean isProd = TardisUtils.isProd();
        UploadLocationSelector uploadLocationSelector = null;

        String host;
        int port, uploadLimit = 2;
        Cookie sessionKey = getSessionKey(mode);
        if (mode == LOCAL)
        {
            if (isProd)
            {
                throw new IllegalArgumentException("cannot connect to local in prod");
            }
            host = "127.0.0.1";
            port = 8080;

            UploadLocation ul = new UploadLocationImpl(
                    new FileLocationImpl("dfd68e76-70ab-52d8-85a4-d401a8dd70b0",
                            "localhost", "tmp", "/tmp", "test", "NONE", 0),
                    "http://127.0.0.1:8737/tardis", null, true);
            uploadLocationSelector = new UploadLocationSelector(Collections.singletonList(ul));
            uploadLimit = 1;
        }
        else
        {
            final String env;
            if (mode == DEV)
            {
                if (isProd)
                {
                    throw new IllegalArgumentException("cannot connect to dev in prod");
                }
                env = "DEV";
            }
            else if (mode == RD)
            {
                env = "DEV";
            }
            else if (mode == PROD)
            {
                env = "PROD";
            }
            else
            {
                throw new RuntimeException("unrecognized mode: " + mode);
            }
            PropertyWrapper pw;
            try
            {
                pw = envs.get(env, new Callable<PropertyWrapper>()
                {
                    @Override
                    public PropertyWrapper call() throws Exception
                    {
                        return new TardisEnv(env).load();
                    }
                });
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException("could not load tardis config for env " + env, e);
            }
            host = pw.getProperty("tardis.host");
            port = Integer.parseInt(pw.getProperty("tardis.port"));
            System.out.println("connecting to " + host + ":" + port);
        }

        //noinspection ConstantConditions
        return new CachedTardisService(
                new TardisWebClient(host, port, sessionKey, null, null, uploadLocationSelector, uploadLimit,
                        connectTimeoutInMillis, readTimeoutInMillis, retryTimeoutInMillis),
                new TenureCache(8,
                        TenureCache.config(TenureCache.Tenure.SHORT, 0, 10, 500),
                        TenureCache.config(TenureCache.Tenure.MEDIUM, 300, 2000),
                        TenureCache.config(TenureCache.Tenure.LONG, 3600, 500),
                        TenureCache.config(TenureCache.Tenure.FOREVER, 86400, 500)));
    }

    @SuppressWarnings("UnusedParameters")
    private static Cookie getSessionKey(int mode)
    {
        // TODO: get session key from auth server
        return null;
    }
}
