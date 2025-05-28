package com.teza.common.tardis.datatypes;

import com.teza.common.qlib.qlib2.ColumnSet;

import java.util.List;

/**
 * User: tom
 * Date: 8/9/17
 * Time: 12:53 PM
 */
public class SampledColumnSet
{

    private final SampledInfo info;
    private final ColumnSet cs;

    public SampledColumnSet(SampledInfo info, ColumnSet cs)
    {
        this.info = info;
        this.cs = cs;
    }

    public ColumnSet getColumnSet()
    {
        return cs;
    }

    public SampledInfo getInfo()
    {
        return info;
    }

    public static SampledColumnSet concat(List<SampledColumnSet> results)
    {
        return null;
    }
}
