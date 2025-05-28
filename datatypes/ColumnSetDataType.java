package com.teza.common.tardis.datatypes;

import com.teza.common.qlib.qlib2.*;
import com.teza.common.util.TypeConverterUtils;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 6:41 PM
 */
public abstract class ColumnSetDataType extends AbstractDataType<ColumnSet>
{
    @Override
    public boolean isNull(ColumnSet val)
    {
        return val == null || val.length() == 0 || val.numColumns() == 0;
    }

    @Override
    public ColumnSet clean(ColumnSet value, DateTime start, DateTime end)
    {
        if (isNull(value))
        {
            return null;
        }
        if (!value.hasColumn("ts"))
        {
            throw new IllegalArgumentException("columnset must contain column ts");
        }
        checkIndex(value.column("ts"),
                start.getMillis() * getMillisMultiplier(),
                end.getMillis() * getMillisMultiplier());
        return value;
    }

    protected abstract long getMillisMultiplier();

    protected boolean checkIndex(Column c, long start, long end)
    {
        long check;
        for (int i = 0; i < c.length(); i++)
        {
            check = c.getLong(i);
            if (check >= end)
            {
                throw new RuntimeException(TypeConverterUtils.millisToDateTime(check / getMillisMultiplier()) +
                        " on row " + i + " right-exceeds " + TypeConverterUtils.millisToDateTime(end / getMillisMultiplier()));
            }
            else if (check < start)
            {
                throw new RuntimeException(TypeConverterUtils.millisToDateTime(check / getMillisMultiplier()) +
                        " on row " + i + " left-exceeds " + TypeConverterUtils.millisToDateTime(start / getMillisMultiplier()));
            }
        }
        return true;
    }

    @Override
    protected ColumnSet getSlice(ColumnSet val, DateTime start, DateTime end)
    {
        final Column ts = val.column("ts");
        Selector filter = ts.between(start.getMillis() * getMillisMultiplier(), end.getMillis() * getMillisMultiplier()).asFilter();
        return val.select(filter);
    }

    public ColumnSet slice(ColumnSet cs, DateTime start, DateTime end)
    {
        return getSlice(cs, start, end);
    }

    @Override
    public void dumps(ColumnSet value, OutputStream os)
    {
        try
        {
            os = getEffectiveOutputStream(os);
            value.toBinaryOutputStream(os);
            os.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected OutputStream getEffectiveOutputStream(OutputStream os) throws IOException
    {
        return new GZIPOutputStream(os);
    }

    @Override
    public JSONObject getMeta(ColumnSet value)
    {
        if (isNull(value))
        {
            return null;
        }
        int columns = value.columns().length();
        if (value.hasColumn("ts"))
        {
            columns--;
        }
        return new JSONObject()
                .put("rows", value.length())
                .put("columns", columns);
    }

    @Override
    public ColumnSet getJoin(List<ColumnSet> results)
    {
        ColumnSet val = null;
        boolean first = true;
        for (ColumnSet s : results)
        {
            if (isNull(s))
                continue;

            if (first)
            {
                val = new ColumnSet();
                val.appendColumns(s);
                first = false;
                continue;
            }
            val.appendColumns(s);
        }
        return val;
    }

    @Override
    public ColumnSet getConcat(List<ColumnSet> results)
    {
        if (results.size() == 1)
        {
            if (isNull(results.get(0)))
            {
                return null;
            }
            return results.get(0);
        }

        ColumnSet val = null, ret;
        boolean first = true;
        ArrayList<String> origNames = new ArrayList<String>();
        String name;
        int numColumns = 0, numRows;
        VArray a;
        for (ColumnSet s : results)
        {
            if (isNull(s))
                continue;

            if (first)
            {
                val = s;
                for (int i : val.columns().indices())
                {
                    name = val.column(i).name();
                    origNames.add(name);
                }
                numColumns = origNames.size();
                first = false;
                continue;
            }

            numRows = s.length();
            ret = new ColumnSet();
            for (int i = 0; i < numColumns; i++)
            {
                name = origNames.get(i);
                if (s.hasColumn(name))
                {
                    a = s.column(name).array();
                }
                else
                {
                    a = QLib.constant(Double.NaN, numRows);
                }
                ret.add(QLib.concat(val.column(i).array(), a).asColumn(name));
            }
            numRows = val.length();
            for (int i : s.columns().indices())
            {
                name = s.column(i).name();
                if (val.hasColumn(name))
                {
                    continue;
                }
                ret.add(QLib.concat(QLib.constant(Double.NaN, numRows), s.column(i).array()).asColumn(name));
                origNames.add(name);
                numColumns++;
            }
            val = ret;
        }
        return val;
    }

    @Override
    public ColumnSet loads(InputStream raw)
    {
        try
        {
            raw = getEffectiveInputStream(raw);
            return ColumnSet.fromBinary(raw);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected InputStream getEffectiveInputStream(InputStream raw) throws IOException
    {
        return new GZIPInputStream(raw);
    }

    public static class TimestampInMicros extends ColumnSetDataType
    {
        @Override
        protected long getMillisMultiplier()
        {
            return 1000L;
        }

        @Override
        public DataTypeKey[] getSupportedCodecs()
        {
            return new DataTypeKey[]{DataTypeFactory.getKey("teza.common.tardis.datatype.dataframe:LegacyColumnSetFile", "0")};
        }

    }

    public static class TimestampInNanos extends ColumnSetDataType
    {
        @Override
        protected long getMillisMultiplier()
        {
            return 1000000L;
        }

        @Override
        public DataTypeKey[] getSupportedCodecs()
        {
            return new DataTypeKey[]{DataTypeFactory.getKey("teza.common.tardis.datatype.dataframe:BackwardsCompatibleColumnSetFile", "0")};
        }
    }

//    public static class Limited extends ColumnSetDataType
//    {
//        @Override
//        public void dumps(ColumnSet value, OutputStream os)
//        {
//            throw new RuntimeException("not supported");
//        }
//
//        @Override
//        public DataTypeKey[] getSupportedCodecs()
//        {
//            return new DataTypeKey[0];
//        }
//
//        @Override
//        protected ColumnSet getSlice(ColumnSet val, DateTime start, DateTime end)
//        {
//            throw new RuntimeException("not supported");
//        }
//
//        @Override
//        public ColumnSet getJoin(List<ColumnSet> results)
//        {
//            throw new RuntimeException("not supported");
//        }
//
//        @Override
//        public ColumnSet getConcat(List<ColumnSet> results)
//        {
//            throw new RuntimeException("not supported");
//        }
//
//    }
}
