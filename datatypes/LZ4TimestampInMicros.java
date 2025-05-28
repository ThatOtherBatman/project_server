package com.teza.common.tardis.datatypes;

import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * User: tom
 * Date: 1/2/18
 * Time: 2:40 PM
 */

public class LZ4TimestampInMicros extends ColumnSetDataType.TimestampInMicros
{
    @Override
    public DataTypeKey[] getSupportedCodecs()
    {
        return new DataTypeKey[]{DataTypeFactory.getKey("teza.common.tardis.datatype.dataframe:LZ4LegacyColumnSetFile", "0")};
    }

    @Override
    protected OutputStream getEffectiveOutputStream(OutputStream os) throws IOException
    {
        return new LZ4FrameOutputStream(os);
    }

    @Override
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
