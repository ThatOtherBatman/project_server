package com.teza.common.tardis.datatypes;

import org.json.JSONArray;
import org.json.JSONObject;

/* You don't really know what class EventListJsonDataType is going send you, other than it's a JsonElement. Didn't want to depend of GSON because GSON is annoying, so here. */
public class EventListElement
{
    protected final Object obj;

    public EventListElement(Object obj)
    {
        this.obj = obj;
    }

    public boolean isNull()
    {
        return obj.equals(JSONObject.NULL);
    }

    public Boolean getBoolean()
    {
        if (isNull())
        {
            return null;
        }
        return (boolean) obj;
    }

    public Double getDouble()
    {
        if (isNull())
        {
            return null;
        }
        return (double) obj;
    }


    public Integer getInt()
    {
        if (isNull())
        {
            return null;
        }
        return (int) obj;
    }

    public JSONArray getJSONArray()
    {
        if (isNull())
        {
            return null;
        }
        return (JSONArray) obj;
    }


    public JSONObject getJSONObject()
    {
        if (isNull())
        {
            return null;
        }
        return (JSONObject) obj;
    }

    public Long getLong()
    {
        if (isNull())
        {
            return null;
        }
        return (long) obj;
    }

    public String getString()
    {
        if (isNull())
        {
            return null;
        }
        return (String) obj;
    }

    public Object get()
    {
        if (isNull())
        {
            return null;
        }
        return obj;
    }
}
