package com.teza.common.tardis;

import java.net.InetAddress;
import java.util.Comparator;
import java.util.List;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 10:18 PM
 */

/**
 * a file in Tardis could be indexed to multiple locations,
 * this is an interface for an object that can pick the best location
 * when accessing files in Tardis
 */
public interface LocationSelector extends Comparator<FileLocation>
{
    FileLocation getBestLocation(List<FileLocation> fileLocations);
}
