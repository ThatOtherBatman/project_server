package com.teza.common.tardis;

import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: tom
 * Date: 1/26/17
 * Time: 12:12 AM
 */
public class PhysicalFile
{
    private final JSONObject fileMeta;
    private final boolean deletable;
    private final AtomicBoolean deleted;
    private final File file;

    public PhysicalFile(String fileName)
    {
        this(fileName, false);
    }

    public PhysicalFile(String fileName, boolean deletable)
    {
        this(fileName, deletable, new JSONObject());
    }

    public PhysicalFile(String fileName, boolean deletable, JSONObject fileMeta)
    {
        file = new File(fileName);
        if (!file.exists())
        {
            throw new IllegalArgumentException(fileName + " does not exist");
        }
        if (fileMeta != null && (!fileMeta.has("upload_file_name") || fileMeta.isNull("upload_file_name")))
        {
            fileMeta.put("upload_file_name", fileName);
        }
        this.fileMeta = fileMeta;
        this.deletable = deletable;
        this.deleted = new AtomicBoolean(false);
    }

    public boolean canDelete()
    {
        return deletable && !deleted.get();
    }

    public String name()
    {
        return file.getAbsolutePath();
    }

    public JSONObject getFileMeta()
    {
        if (fileMeta == null)
        {
            return null;
        }
        return new JSONObject(fileMeta.toString());
    }

    public InputStream getInputStream()
    {
        if (deleted.get())
        {
            throw new RuntimeException("cannot get stream on a deleted file");
        }

        try
        {
            return new FileInputStream(file);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void delete()
    {
        if (deletable)
        {
            if (!file.delete())
            {
                throw new RuntimeException("cannot delete " + file);
            }
            deleted.set(true);
        }
    }
}
