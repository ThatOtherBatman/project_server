package com.teza.common.tardis.handlers;

import java.util.*;

import static com.teza.common.tardis.UUID5.NIL_UUID;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 4:55 PM
 */
public class HandlerResult implements Iterable<DateTimeRange>
{
    private final List<Range> missing = new ArrayList<Range>();
    private final List<Range> found = new ArrayList<Range>();

    HandlerResult(List<Range> missing)
    {
        this.missing.addAll(missing);
    }

    HandlerResult(List<Range> missing, List<Range> found)
    {
        this.missing.addAll(missing);
        this.found.addAll(found);
    }

    public void merge(HandlerResult o)
    {
        if (o == null)
            return;
        missing.clear();
        missing.addAll(o.missing);
        found.addAll(o.found);
    }

    List<Range> getMissing()
    {
        return missing;
    }

    List<Range> getFound()
    {
        return found;
    }

    public List<DateTimeRange> getMissingRanges()
    {
        List<DateTimeRange> ranges = new ArrayList<DateTimeRange>();
        for (Range r : missing)
        {
            ranges.add(new DateTimeRange(r));
        }
        return ranges;
    }

    public List<DateTimeRange> getFoundRanges()
    {
        List<DateTimeRange> ranges = new ArrayList<DateTimeRange>();
        if (found != null)
        {
            for (Range r : found)
            {
                ranges.add(new DateTimeRange(r));
            }
        }
        return ranges;
    }

    public boolean hasMissing()
    {
        return missing.size() > 0;
    }

    public boolean isEmpty()
    {
        return found.isEmpty();
    }

    public void sort()
    {
        Collections.sort(found);
    }

    @Override
    public Iterator<DateTimeRange> iterator()
    {
        if (found == null)
        {
            return new Iterator<DateTimeRange>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public DateTimeRange next()
                {
                    return null;
                }

                @Override
                public void remove()
                {

                }
            };
        }
        final Iterator<Range> iterator = found.iterator();
        return new Iterator<DateTimeRange>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public DateTimeRange next()
            {
                return new DateTimeRange(iterator.next());
            }

            @Override
            public void remove()
            {
                iterator.remove();
            }
        };
    }

    public boolean processCleared()
    {
        List<Range> cleared = new ArrayList<Range>();
        boolean hasCleared;
        for (Range r : found)
        {
            hasCleared = false;
            for (IndexRecord ir : r.getIndexRecords())
            {
                if (ir.getFileUuid().equals(NIL_UUID) && ir.getContent().isEmpty())
                {
                    hasCleared = true;
                    break;
                }
            }
            if (hasCleared)
            {
                cleared.add(r);
                missing.add(r);
            }
        }
        if (cleared.size() > 0)
        {
            Collections.sort(missing);
            found.removeAll(cleared);
            return true;
        }
        return false;
    }
}
