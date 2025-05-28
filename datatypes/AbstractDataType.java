package com.teza.common.tardis.datatypes;

import org.joda.time.DateTime;
import org.json.JSONObject;

import java.util.List;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 6:55 PM
 */
public abstract class AbstractDataType<T> implements DataType<T>
{
    @Override
    public boolean isNull(T val)
    {
        return val == null;
    }

    @Override
    public T getRange(T value, DateTime startTs, DateTime endTs, DateTime dataFromTs, DateTime dataToTs)
    {
        if (isNull(value))
        {
            return null;
        }
        if (!startTs.isAfter(dataFromTs) && !endTs.isBefore(dataToTs))
        {
            return value;
        }
        return getSlice(value, startTs, endTs);
    }

    @Override
    public T join(List<T> results)
    {
        if (results.size() == 1)
        {
            return results.get(0);
        }
        else if (results.isEmpty())
        {
            return null;
        }
        return getJoin(results);
    }

    @Override
    public T concat(List<T> results)
    {
        if (results.size() == 1)
        {
            return results.get(0);
        }
        else if (results.isEmpty())
        {
            return null;
        }
        return getConcat(results);
    }

    @Override
    public boolean isContent(Object value)
    {
        return false;
    }

    @Override
    public Object cleanContent(Object value, DateTime start, DateTime end)
    {
        return value;
    }

    @Override
    public T loadRecord(String content, DateTime start, DateTime end, DateTime knowledge, String fileMeta)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public String dumpContent(Object value)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public JSONObject getContentMeta(Object value)
    {
        return null;
    }

    abstract protected T getSlice(T val, DateTime start, DateTime end);

    abstract protected T getJoin(List<T> results);

    abstract protected T getConcat(List<T> results);

}
