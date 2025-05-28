package com.teza.common.tardis;

import java.util.List;

/**
 * User: tom
 * Date: 1/23/17
 * Time: 8:52 PM
 */
public interface UploadLocation
{
    FileLocation getLocation();
    String getUploadUrl();
    List<String> getPermittedSources();
    boolean isActive();
}
