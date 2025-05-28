package com.teza.common.tardis.datatypes;

import com.teza.common.tardis.PhysicalFile;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.*;
import java.util.List;

/**
 * User: tom
 * Date: 1/26/17
 * Time: 12:11 AM
 */
public class ArbitraryFileDataType extends AbstractDataType<PhysicalFile>
{
    public static final String CODEC = "teza.common.tardis.datatype.arbitraryfile:ArbitraryFile";

    @Override
    protected PhysicalFile getSlice(PhysicalFile val, DateTime start, DateTime end)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    protected PhysicalFile getJoin(List<PhysicalFile> results)
    {
        if (results != null && results.size() == 1)
        {
            return results.get(0);
        }
        throw new RuntimeException("not supported");
    }

    @Override
    protected PhysicalFile getConcat(List<PhysicalFile> results)
    {
        if (results != null && results.size() == 1)
        {
            return results.get(0);
        }
        throw new RuntimeException("not supported");
    }

    @Override
    public PhysicalFile clean(PhysicalFile value, DateTime start, DateTime end)
    {
        return value;
    }

    @Override
    public PhysicalFile loads(InputStream raw)
    {
        File file;
        try
        {
            file = File.createTempFile("__tardisarbi", "__");
            OutputStream os = new FileOutputStream(file);
            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = raw.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.close();
            return new PhysicalFile(file.getAbsolutePath(), true, null);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            if (raw != null) {
                try {
                    raw.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void dumps(PhysicalFile value, OutputStream os)
    {
        InputStream is = value.getInputStream();
        try
        {
            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            is.close();
            os.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JSONObject getMeta(PhysicalFile value)
    {
        return value.getFileMeta();
    }

    @Override
    public DataTypeKey[] getSupportedCodecs()
    {
        return new DataTypeKey[]{DataTypeFactory.getKey(CODEC, "0")};
    }
}
