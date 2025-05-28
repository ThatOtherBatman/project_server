package com.teza.common.tardis;

import java.util.*;

/**
 * User: tom
 * Date: 1/25/17
 * Time: 8:30 PM
 */

public class UploadLocationSelector
{
    public static final int MIN_PRIORITY = -100;
    public static final int ARCHIVE_PRIORITY = 100;
    @SuppressWarnings("unused")
    public static final int MAX_PRIORITY = 1000;
    public static final LocationSelector locationSelector = new LocationSelectorImpl();

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final int minPriority, maxPriority, archivePriority;
    private final HashMap<String, List<DataCenterLocations>> cache = new HashMap<String, List<DataCenterLocations>>();
    private final HashMap<String, List<UploadLocation>> sources = new HashMap<String, List<UploadLocation>>();
    private final ArrayList<UploadLocation> sourceless = new ArrayList<UploadLocation>();

    public UploadLocationSelector(List<UploadLocation> uploadLocations)
    {
        this(uploadLocations, MIN_PRIORITY, MAX_PRIORITY, ARCHIVE_PRIORITY);
    }

    public UploadLocationSelector(List<UploadLocation> uploadLocations, int minPriority, int maxPriority, int archivePriority)
    {
        this.minPriority = minPriority;
        this.maxPriority = maxPriority;
        this.archivePriority = archivePriority;

        Collections.sort(uploadLocations, new Comparator<UploadLocation>()
        {
            @Override
            public int compare(UploadLocation o1, UploadLocation o2)
            {
                return o1.getLocation().getPriority() - o2.getLocation().getPriority();
            }
        });

        for (UploadLocation ul : uploadLocations)
        {
            List<String> permittedSources = ul.getPermittedSources();
            if (permittedSources == null)
            {
                sourceless.add(ul);
            }
            else
            {
                for (String source : permittedSources)
                {
                    if (!sources.containsKey(source))
                    {
                        sources.put(source, new ArrayList<UploadLocation>());
                    }
                    sources.get(source).add(ul);
                }
            }
        }
    }

    public List<DataCenterLocations> getLocations(String source, Set<String> ignoredDataCenters)
    {
        if (source != null && !sources.containsKey(source))
        {
            source = null;
        }
        if (ignoredDataCenters != null)
        {
            return getLocations(source, minPriority, archivePriority, ignoredDataCenters);
        }
        if (!cache.containsKey(source))
        {
            cache.put(source, getLocations(source, minPriority, archivePriority, null));
        }
        return cache.get(source);
    }

    private List<DataCenterLocations> getLocations(String source, int minPriority, int maxPriority, Set<String> ignoredDataCenters)
    {
        Set<String> seen = new HashSet<String>();
        if (source == null)
        {
            return getLocations(sourceless, minPriority, maxPriority, ignoredDataCenters, seen);
        }
        List<DataCenterLocations> dcls = getLocations(sources.get(source), minPriority, maxPriority, ignoredDataCenters, seen);
        for (DataCenterLocations dcl: getLocations(sourceless, minPriority, maxPriority, ignoredDataCenters, seen))
        {
            dcls.add(dcl);
        }
        return dcls;
    }

    private List<DataCenterLocations> getLocations(List<UploadLocation> upLocs, int minPriority, int maxPriority,
                                              Set<String> ignoredDataCenters, Set<String> seen)
    {
        String dc;
        FileLocation loc;
        Map<String, List<UploadLocation>> uploadLocations = new HashMap<String, List<UploadLocation>>();
        List<DataCenterLocations> dcls = new ArrayList<DataCenterLocations>();
        List<FileLocation> locsForLatency = new ArrayList<FileLocation>();
        for (UploadLocation ul : upLocs)
        {
            loc = ul.getLocation();
            if (seen.contains(loc.getFileLocationUuid()))
            {
                continue;
            }
            seen.add(loc.getFileLocationUuid());
            if (!ul.isActive())
            {
                continue;
            }
            if (minPriority > loc.getPriority() || maxPriority < loc.getPriority())
            {
                continue;
            }
            dc = loc.getDataCenter();
            if (ignoredDataCenters != null && ignoredDataCenters.contains(dc))
            {
                continue;
            }
            if (!uploadLocations.containsKey(dc))
            {
                locsForLatency.add(loc);
                uploadLocations.put(dc, new ArrayList<UploadLocation>());
            }
            uploadLocations.get(dc).add(ul);
        }
        Collections.sort(locsForLatency, locationSelector);
        for (FileLocation fl: locsForLatency)
        {
            dc = fl.getDataCenter();
            dcls.add(new DataCenterLocations(dc, uploadLocations.get(dc)));
        }
        return dcls;
    }

    public static class DataCenterLocations
    {
        public String dataCenter;
        public List<UploadLocation> locations;

        public DataCenterLocations(String dataCenter, List<UploadLocation> locations)
        {
            this.dataCenter = dataCenter;
            this.locations = locations;
        }
    }
}
