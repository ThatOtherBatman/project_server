package com.teza.common.tardis.datatypes;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * User: tom
 * Date: 4/10/18
 * Time: 5:06 PM
 */
public class EventListJsonDataType extends AbstractDataType<EventList>
{
    public static final String CODEC = "teza.common.tardis.datatype.tardisjson:EventListLZ4JsonFile";

    @Override
    protected EventList getSlice(EventList val, DateTime start, DateTime end)
    {
        return val.slice(start, end);
    }

    @Override
    protected EventList getJoin(List<EventList> results)
    {
        List<EventList.Item> items = new ArrayList<EventList.Item>();
        results.stream().filter(el -> el != null).forEach(el -> items.addAll(el.getItems()));
        if (items.isEmpty())
        {
            return null;
        }
        return new EventList(items);
    }

    @Override
    protected EventList getConcat(List<EventList> results)
    {
        return getJoin(results);
    }

    @Override
    public EventList clean(EventList value, DateTime start, DateTime end)
    {
        if (value == null)
        {
            throw new NullPointerException("event list object cannot be null");
        }
        for (EventList.Item i : value.getItems())
        {
            if (i.getTimestamp().isBefore(start) || !i.getTimestamp().isBefore(end))
            {
                throw new NullPointerException("item " + i + " has timestamp " + i.getTimestamp() + " outside of [" + start + ", " + end + ")");
            }
        }
        return value;
    }

    @Override
    public EventList loads(InputStream raw)
    {
        try
        {
            raw = getEffectiveInputStream(raw);
            EventList ret = new EventList();
            JSONArray v, data = new JSONArray(new String(IOUtils.toByteArray(raw)));
            for (int i = 0; i < data.length(); i++)
            {
                v = data.getJSONArray(i);
                ret.add(new EventList.Item(new DateTime(v.getLong(0)), v.get(1)));
            }
            return ret;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dumps(EventList value, OutputStream os)
    {
        try
        {
            os = getEffectiveOutputStream(os);
            JSONArray list = new JSONArray();
            for (EventList.Item i : value.getItems())
            {
                list.put(i.toJSONArray());
            }
            os.write(list.toString().getBytes());
            os.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JSONObject getMeta(EventList value)
    {
        if (value == null)
            return null;
        return new JSONObject().put("count", value.length());
    }

    @Override
    public DataTypeKey[] getSupportedCodecs()
    {
        return new DataTypeKey[]{DataTypeFactory.getKey(CODEC, "0")};
    }

    protected OutputStream getEffectiveOutputStream(OutputStream os) throws IOException
    {
        return new LZ4FrameOutputStream(os);
    }

    protected InputStream getEffectiveInputStream(InputStream raw) throws IOException
    {
        try
        {
            return new LZ4FrameInputStream(raw);
        }
        catch (NoSuchMethodError e)
        {
            System.err.println("are you sure the commons-compress-1.14+ jar preceeds other versions of commons-compress?");
            throw e;
        }
    }
}
