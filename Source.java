package com.teza.common.tardis;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 12:32 PM
 */

/**
 * an Enum of accepted sources of data, used as an attribute of Doc to
 * further differentiate one Doc from another.
 *
 * currently used to shard data indexed into Tardis, and therefore
 * CANNOT BE EXTENDED.
 *
 * there are plans to move the sharding to something hidden from user.
 * once moved, this will be an extensible enum of user defined data sources.
 *
 */
public enum Source
{
    GENERAL(1, "general"),
    SAMPLED_FEATURE(2, "sdf"),
    SAMPLED_INSTRUMENT(3, "sdi"),
    PULSAR(4, "pulsar"),
    DERIVED(5, "derived"),
    RAWTIMESERIES(6, "rawts"),
    ALTDATA(7, "alt"),
    RADAR(8, "radar"),
    VENDOR(9, "vendor"),
    TEMP(10, "temp"),
    SECFILE(11, "sec"),
    PERM_RADAR(12, "permradar"),
    ADMIN(13, "admin");

    private final int id;
    private final String shortName;

    Source(int id, String shortName)
    {
        this.id = id;
        this.shortName = shortName;
    }

    public int getId()
    {
        return id;
    }

    public String getShortName()
    {
        return shortName;
    }
}
