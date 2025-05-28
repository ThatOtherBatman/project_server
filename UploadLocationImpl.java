package com.teza.common.tardis;

import java.util.Arrays;
import java.util.List;

/**
 * User: tom
 * Date: 1/23/17
 * Time: 8:56 PM
 */
public class UploadLocationImpl implements UploadLocation
{
    private final FileLocation location;
    private final String uploadUrl, permittedSources;
    private final boolean active;

    public UploadLocationImpl(FileLocation location, String uploadUrl, String permittedSources, boolean active)
    {
        this.location = location;
        this.uploadUrl = uploadUrl;
        this.permittedSources = permittedSources;
        this.active = active;
    }

    @Override
    public FileLocation getLocation()
    {
        return location;
    }

    @Override
    public String getUploadUrl()
    {
        return uploadUrl;
    }

    @Override
    public List<String> getPermittedSources()
    {
        if (permittedSources == null || permittedSources.isEmpty())
        {
            return null;
        }
        return Arrays.asList(permittedSources.split(","));
    }

    @Override
    public boolean isActive()
    {
        return active;
    }
}
