package com.teza.common.tardis.datatypes;

import com.teza.common.qlib.qlib2.*;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
//import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: tom
 * Date: 8/9/17
 * Time: 12:53 PM
 */
public class SampledDataFile extends AbstractDataType<SampledColumnSet>
{
    protected static final ColumnSetDataType columnSetDataType = new ColumnSetDataType()
    {
        @Override
        protected long getMillisMultiplier()
        {
            return 1000L;
        }

        @Override
        public DataTypeKey[] getSupportedCodecs()
        {
            return null;
        }
    };

    @Override
    public boolean isNull(SampledColumnSet value)
    {
        return value == null || columnSetDataType.isNull(value.getColumnSet());
    }

    @Override
    public SampledColumnSet clean(SampledColumnSet value, DateTime start, DateTime end)
    {
        if (value == null)
        {
            return null;
        }
        columnSetDataType.clean(value.getColumnSet(), start, end);
        return value;
    }

    @Override
    public SampledColumnSet getSlice(SampledColumnSet value, DateTime start, DateTime end)
    {
        ColumnSet original = value.getColumnSet();
        ColumnSet cs = columnSetDataType.slice(original, start, end);
        if (cs.length() == 0)
        {
            return null;
        }
        if (cs.length() == original.length())
        {
            return value;
        }
        LocalDate startDate = SampledInfo.getStartDate(start);
        LocalDate endDate = SampledInfo.getEndDate(end);
        if (!startDate.isBefore(endDate))
        {
            endDate = SampledInfo.getNextDate(endDate);
        }
        SampledInfo info = value.getInfo() == null ? null : value.getInfo().getRange(startDate, endDate);
        return new SampledColumnSet(info, cs);
    }

    @Override
    protected SampledColumnSet getJoin(List<SampledColumnSet> results)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected SampledColumnSet getConcat(List<SampledColumnSet> results)
    {
        List<SampledInfo> infos = new ArrayList<>();
        List<ColumnSet> data = new ArrayList<>();
        for (SampledColumnSet scs : results)
        {
            infos.add(scs.getInfo());
            data.add(scs.getColumnSet());
        }
        return new SampledColumnSet(SampledInfo.concat(infos), concatColumnSet(data));
    }

    public ColumnSet concatColumnSet(List<ColumnSet> results)
    {
        if (results == null || results.size() == 0)
        {
            return null;
        }
        else if (results.size() == 1)
        {
            if (columnSetDataType.isNull(results.get(0)))
            {
                return null;
            }
            return results.get(0);
        }

        List<ColumnSet> concats = new ArrayList<>();
        List<String> origNames = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        int rows = 0;
        for (ColumnSet s : results)
        {
            if (columnSetDataType.isNull(s))
                continue;

            if (!s.hasColumn("ts"))
            {
                throw new RuntimeException("cannot found column ts in columnset");
            }
            else if (s.numColumns() == 1)
            {
                continue;
            }

            concats.add(s);
            rows += s.length();
            for (int i : s.columns().indices())
            {
                String name = s.column(i).name();
                if (name.equals("ts"))
                {
                    continue;
                }
                if (seen.contains(name))
                {
                    continue;
                }
                origNames.add(name);
                seen.add(name);
            }
        }

        if (concats.size() == 0)
        {
            return null;
        }

//        System.out.println(new DateTime(DateTimeZone.UTC) + " making " + results.size() + " columnsets the same shape..");
        long[] ts = new long[rows];
        double[][] values = new double[origNames.size()][];
        for (int i = 0; i < origNames.size(); i++)
        {
            values[i] = new double[rows];
        }

        int offset = 0;
        for (ColumnSet s : concats)
        {
            for (long l : s.column("ts").toLongs())
            {
                ts[offset++] = l;
            }
        }

//        System.out.println(new DateTime(DateTimeZone.UTC) + " stacking " + results.size() + " columnsets to arrays..");
        for (int j = 0; j < values.length; j++)
        {
            offset = 0;
            String name = origNames.get(j);
            double[] v = values[j];
            for (ColumnSet s : concats)
            {
                if (s.hasColumn(name))
                {
                    for (double d : s.column(name).toDoubles())
                    {
                        v[offset++] = d;
                    }
                }
                else
                {
                    int i = 0, count = s.length();
                    while (i++ < count)
                    {
                        v[offset++] = Double.NaN;
                    }
                }
            }
        }

//        System.out.println(new DateTime(DateTimeZone.UTC) + " materializing " + results.size() + " columnsets to one..");
        ColumnSet ret = new ColumnSet();
        ret.add(VArray.referenceArray(ts).asColumn("ts"));
        for (int j = 0; j < values.length; j++)
        {
            String name = origNames.get(j);
            ret.add(VArray.referenceArray(values[j]).asColumn(name));
        }
//        System.out.println(new DateTime(DateTimeZone.UTC) + " returning final columnset");
        return ret;
    }

    @Override
    public SampledColumnSet loads(InputStream raw)
    {
        try
        {
            InputStream stream = getEffectiveInputStream(raw);
            return loadsWithCheck(stream);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected InputStream getEffectiveInputStream(InputStream raw) throws IOException
    {
        return raw;
    }

    @Override
    public void dumps(SampledColumnSet value, OutputStream os)
    {
        try
        {
            OutputStream stream = getEffectiveOutputStream(os);
            dumpsWithCheck(value, stream);
            os.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected OutputStream getEffectiveOutputStream(OutputStream os) throws IOException
    {
        return os;
    }

    @Override
    public JSONObject getMeta(SampledColumnSet value)
    {
        if (value == null)
        {
            return null;
        }
        return columnSetDataType.getMeta(value.getColumnSet());
    }

    public void checkReadWrite()
    {
        long ts = 0L;
        int tezaId = -1;
        double value = 100.;
        ColumnSet csIn = new ColumnSet(), csOut;
        csIn.add("ts", QLib.constant(ts, 1));
        csIn.add(Integer.toString(tezaId), QLib.constant(value, 1));
        List<SampleInstrument> instruments = new ArrayList<>();
        instruments.add(new SampleInstrument("a", "teza_product", tezaId, 0, "primary", "domain").addTezaAlias("a"));
        SampledInfo info = new SampledInfo(new LocalDate(1970, 1, 1), instruments);
        SampledColumnSet input = new SampledColumnSet(info, csIn);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        dumps(input, os);
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        SampledColumnSet output = loads(is);

        assert (output.getInfo().equals(input.getInfo()));
        csOut = output.getColumnSet();
        assert (csIn.length() == csOut.length());
        assert (csIn.names().toString().equals(csOut.names().toString()));
        assert (csOut.column("ts").getLong(0) == ts);
        assert (csOut.column(Integer.toString(tezaId)).getLong(0) == value);
    }

    @Override
    public DataTypeKey[] getSupportedCodecs()
    {
        return new DataTypeKey[0];
    }

    private static final Codecs.UncompressibleStringCodec stringCodec = new Codecs.UncompressibleStringCodec();
    private static final int singleWriteThreshold = 11796480;
    private static final int minorVersion = 0;
    private static final String version = "2." + Integer.toString(minorVersion);

    public static void dumpsWithCheck(SampledColumnSet scs, OutputStream outputStream) throws IOException
    {
        ColumnSet cs = scs.getColumnSet();
        SampledInfo info = scs.getInfo();
        byte[] infoBytes;
        if (info == null)
        {
            infoBytes = new byte[0];
        }
        else
        {
            infoBytes = info.serialize().getBytes("UTF-8");
        }

        if (!cs.hasColumn("ts"))
        {
            throw new IllegalArgumentException("invalid columnset, must include \"ts\" column");
        }

        outputStream = new BufferedOutputStream(outputStream);
        WritableByteChannel channel = Channels.newChannel(outputStream);

        Codecs.Codec codec;
        JSONObject header = new JSONObject();
        JSONArray configs = new JSONArray();
        ObjectArray<Column> columns = cs.columns();
        ArrayList<String> names = new ArrayList<>();
        final int length = cs.length();

        long offset = 0;
        VArray v = cs.column("ts").optimize();
        try
        {
            codec = v.defaultCodec();
            if (!(codec instanceof Codecs.LongCodec))
            {
                throw new RuntimeException("ts column must be longs, found: " + codec);
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Error getting default codec for ts", t);
        }
        configs.put(new JSONObject().put("name", "ts").put("codec", codec.getClass().getName()).put("offset", offset));
        offset += codec.getEncodedLength(v);

        for (Column c : columns)
        {
            if (c.name().equals("ts"))
            {
                continue;
            }
            names.add(c.name());
            v = c.optimize();
            try
            {
                codec = v.defaultCodec();
                if (!(codec instanceof Codecs.DoubleCodec))
                {
                    throw new RuntimeException("non-ts columns must be doubles, found: " + codec);
                }
            }
            catch (Throwable t)
            {
                throw new RuntimeException("Error getting default codec for " + c.name(), t);
            }
            configs.put(new JSONObject().put("name", c.name()).put("codec", codec.getClass().getName()).put("offset", offset));
            offset += codec.getEncodedLength(v);
        }

        header.put("version", version);
        header.put("columns", configs);
        header.put("length", length);

        final byte[] headerBytes = header.toString().getBytes();
        long columnOffset = 8 + headerBytes.length;
        VArray nameArray = VArray.from(names);
        long nameSize = stringCodec.getEncodedLength(nameArray);
        long dataStart = columnOffset + 20 + nameSize + infoBytes.length;
        if (dataStart > Integer.MAX_VALUE)
            throw new RuntimeException("Planned for integers only...");

        ByteBuffer buffer = ByteBuffer.allocate((int) dataStart);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.putInt((int) dataStart);
        buffer.putInt(headerBytes.length);
        buffer.put(headerBytes);
        buffer.putInt(length);
        buffer.putInt(names.size());
        buffer.putInt((int) nameSize);
        buffer.putInt(infoBytes.length);
        buffer.putInt(minorVersion);
        stringCodec.write(nameArray, buffer);
        buffer.put(infoBytes);
        buffer.rewind();
        channel.write(buffer);

        v = cs.column("ts").times(1000L).optimize();
        long encodedLength = Codecs.longCodec.getEncodedLength(v);
        if (encodedLength > Integer.MAX_VALUE)
            throw new RuntimeException("Planned for integers only...");
        buffer = ByteBuffer.allocate((int) encodedLength);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        Codecs.longCodec.write(v, buffer);
        buffer.rewind();
        channel.write(buffer);

        if (encodedLength * names.size() <= singleWriteThreshold)
        {
            buffer = ByteBuffer.allocate((int) encodedLength * names.size());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            for (String name : names)
            {
                Codecs.doubleCodec.write(cs.column(name).optimize(), buffer);
            }
            buffer.rewind();
            channel.write(buffer);
        }
        else
        {
            for (String name : names)
            {
                buffer = ByteBuffer.allocate((int) encodedLength);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                Codecs.doubleCodec.write(cs.column(name).optimize(), buffer);
                buffer.rewind();
                channel.write(buffer);
            }
        }
        channel.close();
    }

    public static SampledColumnSet loadsWithCheck(InputStream stream) throws IOException
    {
        ColumnSet ret = new ColumnSet();
        ByteBuffer bytes = ByteBuffer.wrap(IOUtils.toByteArray(stream));
        bytes.order(ByteOrder.LITTLE_ENDIAN);
        //noinspection unused
        int dataOffset = bytes.getInt();
        int headerLength = bytes.getInt();
        bytes.position(8 + headerLength);
        int length = bytes.getInt();
        int numCols = bytes.getInt();
        //noinspection unused
        int nameSize = bytes.getInt();
        int infoSize = bytes.getInt();
        //noinspection unused
        int minorVersion = bytes.getInt();
        VArray names = stringCodec.read(bytes, numCols);
        SampledInfo info;
        if (infoSize == 0)
        {
            info = null;
        }
        else
        {
            byte[] infoBytes = new byte[infoSize];
            bytes.get(infoBytes);
            info = SampledInfo.deserialize(new String(infoBytes));
        }
        ret.add("ts", Codecs.longCodec.read(bytes, length).divideLong(1000L));
        for (int i = 0; i < names.length(); i++)
        {
            ret.add(names.getString(i), Codecs.doubleCodec.read(bytes, length));
        }
        return new SampledColumnSet(info, ret);
    }
}
