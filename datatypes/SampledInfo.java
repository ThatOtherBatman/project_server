package com.teza.common.tardis.datatypes;

import com.teza.common.util.Datetime;
import com.teza.common.util.TimeRange;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * User: tom
 * Date: 8/9/17
 * Time: 1:35 PM
 */

public class SampledInfo
{
    private static final TimeRange tardisTimeRange = new TimeRange("23:00:00 UTC,-1d", "23:00:00 UTC");

    private final List<Range> ts;
    public final LocalDate start, end;

    public SampledInfo(LocalDate date, List<SampleInstrument> instruments)
    {
        this(new Range[]{new Range(date, instruments)});
    }

    public SampledInfo(Range[] ranges)
    {
        Range last = null;
        ts = new ArrayList<>();

        if (ranges == null || ranges.length == 0)
        {
            throw new IllegalArgumentException("info ranges cannot be empty");
        }

        for (Range r : ranges)
        {
            if (last != null && last.end.isAfter(r.start))
            {
                throw new IllegalArgumentException("invalid timeseries found: " + last + " --> " + r);
            }

            if (last == null || !last.end.equals(r.start) || !last.hasSameMapping(r))
            {
                last = r.copy();
                ts.add(last);
            }
            else
            {
                last.end = r.end;
            }
        }
        start = ranges[0].start;
        end = ranges[ranges.length - 1].end;
    }

    @Override
    public String toString()
    {
        return "SampledInfo<" + start + ", " + end + ": " + ts + ">";
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null || !(other instanceof SampledInfo))
        {
            return false;
        }

        SampledInfo o = (SampledInfo) other;

        if (start.equals(o.start) && end.equals(o.end) && ts.size() == o.ts.size())
        {
            for (int i = 0; i < ts.size(); i++)
            {
                if (!ts.get(i).equals(o.ts.get(i)))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private SampledInfo(List<Range> ranges)
    {
        if (ranges == null || ranges.isEmpty())
        {
            throw new RuntimeException("must be a slice");
        }

        ts = ranges;
        start = ts.get(0).start;
        end = ts.get(ts.size() - 1).end;
    }

    public SampledInfo getRange(LocalDate start, LocalDate end)
    {
        if (!start.isBefore(end))
        {
            return null;
        }
        List<Range> slice = new ArrayList<>();
        boolean changed = false;
        for (Range r : ts)
        {
            if (!r.end.isAfter(start))
            {
                changed = true;
                continue;
            }
            else if (!r.start.isBefore(end))
            {
                changed = true;
                break;
            }
            r = r.copy();
            if (r.start.isBefore(start))
            {
                changed = true;
                r.start = start;
            }
            if (r.end.isAfter(end))
            {
                changed = true;
                r.end = end;
            }
            slice.add(r);
        }
        if (!changed)
        {
            return this;
        }
        if (slice.isEmpty())
        {
            return null;
        }
        return new SampledInfo(slice);
    }

    public String serialize()
    {
        if (ts.isEmpty())
        {
            return "";
        }
        JSONArray ret = new JSONArray();
        for (Range r : ts)
        {
            ret.put(r.toJson());
        }
        return ret.toString();
    }

    public static SampledInfo deserialize(String v)
    {
        List<Range> ret = new ArrayList<>();
        if (!v.isEmpty())
        {
            JSONArray a = new JSONArray(v);
            for (int i = 0; i < a.length(); i++)
            {
                ret.add(Range.fromJson(a.getJSONObject(i)));
            }
        }
        return new SampledInfo(ret);
    }

    public static SampledInfo concat(List<SampledInfo> infos)
    {
        if (infos == null || infos.size() == 0)
        {
            return null;
        }
        else if (infos.size() == 1)
        {
            return infos.get(0);
        }

        List<Range> ranges = new ArrayList<>();
        boolean isFirst;
        Range last = null;
        for (SampledInfo i : infos)
        {
            if (i == null)
            {
                continue;
            }
            if (last == null)
            {
                for (Range r : i.ts)
                {
                    last = r.copy();
                    ranges.add(last);
                }
            }
            else
            {
                i = i.getRange(last.end, i.end);
                if (i == null)
                {
                    continue;
                }
                isFirst = true;
                for (Range r : i.ts)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                        if (last.hasSameMapping(r))
                        {
                            last.end = r.end;
                            continue;
                        }
                    }
                    last = r.copy();
                    ranges.add(last);
                }
            }
        }
        if (ranges.size() == 0)
        {
            return null;
        }
        return new SampledInfo(ranges);
    }

    public static LocalDate getNextDate(LocalDate date)
    {
        if (date.getDayOfWeek() == 5)
        {
            return date.plusDays(3);
        }
        return date.plusDays(1);
    }

    public static DateTime getStartDateTime(LocalDate date)
    {
        int dow = date.dayOfWeek().get();
        if (dow > 5)
        {
            date = date.plusDays(8 - dow);
        }
        return new DateTime(tardisTimeRange.getDatetimeStart(date).toMicrosecs() / 1000L, DateTimeZone.UTC);
    }

    public static DateTime getEndDateTime(LocalDate date)
    {
        return getStartDateTime(getNextDate(date));
    }

    public static LocalDate getStartDate(DateTime dt)
    {
        dt = new DateTime(dt.getMillis(), DateTimeZone.UTC);
        if (dt.hourOfDay().get() == 23)
        {
            return dt.toLocalDate().plusDays(1);
        }
        return dt.toLocalDate();
    }

    public static LocalDate getEndDate(DateTime dt)
    {
        LocalDate date = getStartDate(dt);
        DateTime dayBreak = getStartDateTime(date);
        if (dt.equals(dayBreak))
        {
            return date;
        }
        return getNextDate(date);
    }

    public static class Range
    {
        public JSONObject mapping = new JSONObject();
        public LocalDate start, end;

        private Range(LocalDate start, LocalDate end,
                     JSONObject mapping)
        {
            this.start = start;
            this.end = end;
            this.mapping = mapping;
        }

        public Range(LocalDate date, List<SampleInstrument> instruments)
        {
            this(date, getNextDate(date), instruments);
        }

        public Range(LocalDate start, LocalDate end, List<SampleInstrument> instruments)
        {
            this.start = start;
            this.end = end;
            for (SampleInstrument i : instruments)
            {
                for (String s : i.getTezaAliases())
                {
                    mapping.put(s, i.getTezaId());
                }
            }
        }

        public boolean hasSameMapping(Range other)
        {
            return mapping.toString().equals(other.mapping.toString());
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == null || !(other instanceof Range))
            {
                return false;
            }
            Range o = (Range) other;
            return start.equals(o.start) && end.equals(o.end) && hasSameMapping(o);
        }

        public Range copy()
        {
            return new Range(start, end, mapping);
        }

        public String toString()
        {
            return "SampledInfo$Range(" + start + ", " + end + ", <mapping of size " + mapping.length() + ">)";
        }

        public JSONObject toJson()
        {
            return new JSONObject().put("s", Datetime.toIntDate(start))
                    .put("e", Datetime.toIntDate(end))
                    .put("m", mapping);
        }

        public static Range fromJson(JSONObject json)
        {
            return new Range(Datetime.toLocalDate(json.getInt("s")),
                    Datetime.toLocalDate(json.getInt("e")),
                    json.getJSONObject("m"));
        }
    }
}
