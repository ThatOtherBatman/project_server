package com.teza.common.tardis.datatypes;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User: tom
 * Date: 4/10/18
 * Time: 5:10 PM
 */
public class EventList
{
    private final List<Item> items;

    public EventList()
    {
        items = new ArrayList<Item>();
    }

    public EventList(List<Item> items)
    {
        this();
        items.forEach(this::add);
    }

    public EventList slice(DateTime start, DateTime end)
    {
        List<Item> sliceItems = new ArrayList<Item>();
        for (Item i : items)
        {
            if (i.getTimestamp().isBefore(start))
            {
                continue;
            }
            else if (!i.getTimestamp().isBefore(end))
            {
                break;
            }
            sliceItems.add(i);
        }
        if (sliceItems.isEmpty())
        {
            return null;
        }
        return new EventList(sliceItems);
    }

    public void add(Item item)
    {
        if (items.size() == 0 || !item.getTimestamp().isBefore(items.get(items.size() - 1).getTimestamp()))
        {
            items.add(item);
        }
        else
        {
            throw new IllegalArgumentException("cannot add item " + item);
        }
    }

    public void add(DateTime dt, Object obj)
    {
        Item item = new Item(dt, obj);
        items.add(item);
    }

    public List<Item> getItems()
    {
        return Collections.unmodifiableList(items);
    }

    public int length()
    {
        return items.size();
    }

    public String toString()
    {
        return "EventList<" + items + ">";
    }

    public static class Item extends EventListElement
    {
        private final long ts;

        public Item(DateTime dt, Object object)
        {
            super(object);
            this.ts = dt.getMillis() * 1000000L;
        }

        public DateTime getTimestamp()
        {
            return new DateTime(ts / 1000000L, DateTimeZone.UTC);
        }

        public JSONArray toJSONArray()
        {
            return new JSONArray().put(ts).put(obj);
        }

        public String toString()
        {
            return "EventList.Item<" + getTimestamp() + ", " + obj + ">";
        }
    }
}
