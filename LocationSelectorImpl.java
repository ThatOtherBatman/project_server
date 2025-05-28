package com.teza.common.tardis;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.teza.common.util.NetworkEndpointUtils;
import com.teza.common.util.Site;
import com.teza.common.util.latency.NetworkLatency;
import com.teza.common.util.latency.NetworkLatencyCalculatorImpl;
import org.joda.time.LocalDate;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 10:22 PM
 */
public class LocationSelectorImpl implements LocationSelector
{
    private static final int MAX_LATENCY = 1000000;
    private static final NetworkLatencyCalculatorImpl latencyCalculator = NetworkLatency.tezaDefaultLatenciesCalculator();
    private final Cache<String, Integer> latencies = CacheBuilder.newBuilder().expireAfterWrite(3600, TimeUnit.SECONDS).build();
    private final Cache<String, Site> sites = CacheBuilder.newBuilder().build();
    private final Site localSite;
    private final LocalDate date;

    public LocationSelectorImpl()
    {
        this(new LocalDate());
    }

    public LocationSelectorImpl(LocalDate date)
    {
        Site site;
        try
        {
            site = Site.from(NetworkEndpointUtils.getNetworkEndpointForHost(InetAddress.getLocalHost()));
        }
        catch (Throwable e)
        {
            site = Site.NONE;
        }
        localSite = site;
        this.date = date;
    }

    @Override
    public FileLocation getBestLocation(List<FileLocation> fileLocations)
    {
        if (fileLocations == null || fileLocations.isEmpty())
        {
            return null;
        }
        Collections.sort(fileLocations, this);
        return fileLocations.get(0);
    }

    @Override
    public int compare(FileLocation o1, FileLocation o2)
    {
        int check = o1.getPriority() - o2.getPriority();
        if (check != 0 || localSite == Site.NONE)
            return check;
        return getDataCenterLatency(o1) - getDataCenterLatency(o2);
    }

    private int getDataCenterLatency(final FileLocation fl)
    {
        final String dc = fl.getDataCenter();
        if (dc.equals("NONE"))
        {
            return 0;
        }
        try
        {
            return latencies.get(dc, new Callable<Integer>()
            {
                @Override
                public Integer call() throws Exception
                {
                    Site site = sites.get(dc, new Callable<Site>()
                    {
                        @Override
                        public Site call() throws Exception
                        {
                            try
                            {
                                return Site.from(NetworkEndpointUtils.getNetworkEndpointForHost(fl.getServer()));
                            }
                            catch (Throwable e)
                            {
                                System.out.println("ignoring site error for server " + fl.getServer() + ": " + e.getMessage());
                                return Site.NONE;
                            }
                        }
                    });
                    try
                    {
                        return latencyCalculator.computeLatency(localSite, site, date);
                    }
                    catch (Throwable e)
                    {
                        System.out.println("ignoring latency calculation error: " + e.getMessage());
                        return MAX_LATENCY;
                    }
                }
            });
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
