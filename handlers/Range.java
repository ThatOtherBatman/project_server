package com.teza.common.tardis.handlers;

import java.util.ArrayList;
import java.util.List;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 5:37 PM
 */
public class Range implements Comparable<Range>
{
    private final long startTs, endTs;
    private List<IndexRecord> indexRecords = null;

    Range(long startTs, long endTs)
    {
        this.startTs = startTs;
        this.endTs = endTs;
    }

    Range(long startTs, long endTs, IndexRecord ir)
    {
        this(startTs, endTs);
        indexRecords = new ArrayList<IndexRecord>();
        indexRecords.add(ir);
    }

    Range(long startTs, long endTs, List<IndexRecord> indexRecords)
    {
        this(startTs, endTs);
        this.indexRecords = indexRecords;
    }

    Range(long startTs, long endTs,
          List<IndexRecord> indexRecords1, List<IndexRecord> indexRecords2)
    {
        this(startTs, endTs);
        indexRecords = new ArrayList<IndexRecord>();
        indexRecords.addAll(indexRecords1);
        indexRecords.addAll(indexRecords2);
    }

    long getStartTs()
    {
        return startTs;
    }

    long getEndTs()
    {
        return endTs;
    }

    List<IndexRecord> getIndexRecords()
    {
        return indexRecords;
    }

    @Override
    public int compareTo(Range o)
    {
        return (startTs < o.startTs) ? -1 : (
                (startTs > o.startTs) ? 1 : (
                        (endTs < o.endTs) ? -1 : (
                                (endTs > o.endTs) ? 1 : 0)));
    }
}
