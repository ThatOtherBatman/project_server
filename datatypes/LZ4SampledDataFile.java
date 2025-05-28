package com.teza.common.tardis.datatypes;

//import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
//import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * User: tom
 * Date: 7/26/17
 * Time: 12:32 PM
 */
public class LZ4SampledDataFile extends SampledDataFile
{
    @Override
    public DataTypeKey[] getSupportedCodecs()
    {
        return new DataTypeKey[]{DataTypeFactory.getKey("teza.common.tardis.datatype.dataframe:LZ4SampledDataFile", "0")};
    }

    @Override
    protected OutputStream getEffectiveOutputStream(OutputStream os) throws IOException
    {
        return new LZ4FrameOutputStream(os);
//        return new FramedLZ4CompressorOutputStream(os);
//        return new FramedLZ4CompressorOutputStream(os,
//                new FramedLZ4CompressorOutputStream.Parameters(FramedLZ4CompressorOutputStream.BlockSize.K256, false, false, false));
    }

    @Override
    protected InputStream getEffectiveInputStream(InputStream raw) throws IOException
    {
        try
        {
            return new LZ4FrameInputStream(raw);
//            return new FramedLZ4CompressorInputStream(raw);
        }
        catch (NoSuchMethodError e)
        {
            System.err.println("are you sure the commons-compress-1.14+ jar preceeds other versions of commons-compress?");
            throw e;
        }
    }
}
