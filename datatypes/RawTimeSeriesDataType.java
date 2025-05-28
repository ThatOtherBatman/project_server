package com.teza.common.tardis.datatypes;

import com.teza.common.qlib.qlib2.Column;
import com.teza.common.qlib.qlib2.ColumnSet;
import com.teza.common.qlib.qlib2.QLib;
import org.joda.time.DateTime;

/**
 * User: tom
 * Date: 1/26/17
 * Time: 3:21 PM
 */
public abstract class RawTimeSeriesDataType<T> extends ColumnSetDataType.TimestampInMicros
{
    public abstract T parseContent(String content);
    public abstract ColumnSet valueToColumnSet(long ts, T value);

    @Override
    public ColumnSet loadRecord(String content, DateTime start, DateTime end, DateTime knowledge, String fileMeta)
    {
        T value = parseContent(content);
        return valueToColumnSet(start.getMillis() * 1000, value);
    }

    public static class Doubles extends RawTimeSeriesDataType<Double>
    {
        public static final String CODEC = "teza.common.tardis.datatype.rawtimeseries:DoublesFile";

        @Override
        public boolean isContent(Object value)
        {
            return value instanceof Double;
        }

        @Override
        public String dumpContent(Object value)
        {
            return value.toString();
        }

        @Override
        public DataTypeKey[] getSupportedCodecs()
        {
            return new DataTypeKey[]{DataTypeFactory.getKey(CODEC, "0")};
        }

        @Override
        public Double parseContent(String content)
        {
            return Double.parseDouble(content);
        }

        @Override
        public ColumnSet valueToColumnSet(long ts, Double value)
        {
            ColumnSet ret = new ColumnSet();
            ret.add(new Column("ts", QLib.longs(ts)));
            ret.add(new Column("value", QLib.doubles(value)));
            return ret;
        }
    }
}
