package com.teza.common.tardis.caches;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.teza.common.codegen.pojo.PojoFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * User: robin
 * Date: 12/16/16
 * Time: 3:11 PM
 */
public class TenureCache<K,V>
{
    public enum Tenure
    {
        SHORT, MEDIUM, LONG,

        /* MAX_VALUE*/
        FOREVER
    }

    public interface TenureConfig
    {
        Tenure getTenure(); TenureConfig setTenure(Tenure value);
        long getExpireAfterAccessSeconds(); TenureConfig setExpireAfterAccessSeconds(long value);
        long getExpireAfterWriteSeconds(); TenureConfig setExpireAfterWriteSeconds(long value);
        long getMaxSize(); TenureConfig setMaxSize(long value);
    }

    public static TenureConfig config(Tenure tenure, long expireAfterAccessSeconds, long maxSize)
    {
        return config(tenure, expireAfterAccessSeconds, 0, maxSize);
    }

    public static TenureConfig config(Tenure tenure, long expireAfterAccessSeconds, long expireAfterWriteSeconds, long maxSize)
    {
        return PojoFactory.make(TenureConfig.class)
                .setTenure(tenure)
                .setExpireAfterAccessSeconds(expireAfterAccessSeconds)
                .setExpireAfterWriteSeconds(expireAfterWriteSeconds)
                .setMaxSize(maxSize);
    }

    private final Cache<K,V>[] caches;

    public TenureCache(int concurrency, TenureConfig ... configs)
    {
        //noinspection unchecked
        caches = new Cache[Tenure.FOREVER.ordinal()+1];

        for(TenureConfig tc : configs)
        {
            caches[tc.getTenure().ordinal()] = CacheBuilder.newBuilder()
                    .concurrencyLevel(concurrency)
                    .expireAfterAccess(tc.getExpireAfterAccessSeconds(), TimeUnit.SECONDS)
                    .expireAfterWrite(tc.getExpireAfterWriteSeconds(), TimeUnit.SECONDS)
                    .maximumSize(tc.getMaxSize())
                    .build();
        }

        Cache next = null;
        for(int i = caches.length-1; i >= 0; i--)
        {
            if(caches[i] == null)
            {
                Cache previous = getPreviousNonNullEntry(caches, i - 1);
                if(previous == null) previous = next;
                //noinspection unchecked
                caches[i] = previous;
            }
            next = caches[i];
        }
    }

    private static Cache getPreviousNonNullEntry(Cache[] cc, int from)
    {
        for(int i = from; i >= 0; i--)
        {
            if(cc[i] != null)
            {
                return cc[i];
            }
        }

        return null;
    }

    public V getIfPresent(Tenure tenure, K key)
    {
        //noinspection RedundantCast
        return caches[tenure.ordinal()].getIfPresent(key);
    }

    public V get(Tenure tenure, K key, Callable<V> handler) throws Exception
    {
        return caches[tenure.ordinal()].get(key, handler);
    }

    public void put(Tenure tenure, K key, V value) throws Exception
    {
        caches[tenure.ordinal()].put(key, value);
    }

    public void invalidate(Tenure tenure, K key)
    {
        caches[tenure.ordinal()].invalidate(key);
    }

    public void invalidateAll(Tenure tenure)
    {
        caches[tenure.ordinal()].invalidateAll();
    }

    public void invalidateAll()
    {
        for (Cache c : caches)
        {
            c.invalidateAll();
        }
    }

    public CacheStats stats()
    {
        CacheStats cs;
        long hitCount = 0, missCount = 0, loadSuccessCount = 0, loadExceptionCount = 0, totalLoadTime = 0, evictionCount = 0;
        for (Cache c : caches)
        {
            cs = c.stats();
            hitCount += cs.hitCount();
            missCount += cs.missCount();
            loadSuccessCount += cs.loadSuccessCount();
            loadExceptionCount += cs.loadExceptionCount();
            totalLoadTime += cs.totalLoadTime();
            evictionCount += cs.evictionCount();
        }
        return new CacheStats(hitCount, missCount, loadSuccessCount,
                loadExceptionCount, totalLoadTime, evictionCount);
    }

    public static void main(String[] args)
    {
        TenureCache c = new TenureCache(1, config(Tenure.SHORT, 1,2), config(Tenure.MEDIUM, 1,2), config(Tenure.LONG, 1,2), config(Tenure.FOREVER, 1,2));

        for(Cache cache : c.caches)
        {
            if(cache == null)
            {
                throw new RuntimeException("no cache!");
            }

        }
    }
}
