package com.teza.common.tardis;

/**
 * User: tom
 * Date: 5/20/17
 * Time: 3:15 PM
 */
public class FileLocationRelImpl implements FileLocationRel
{
    private String docUuid, fileUuid, fileLocationUuid;

    public FileLocationRelImpl(String docUuid, String fileUuid, String fileLocationUuid)
    {
        this.docUuid = docUuid;
        this.fileUuid = fileUuid;
        this.fileLocationUuid = fileLocationUuid;
    }

    @Override
    public String getDocUuid()
    {
        return docUuid;
    }

    @Override
    public void setDocUuid(String v)
    {
        docUuid = v;
    }

    @Override
    public String getFileUuid()
    {
        return fileUuid;
    }

    @Override
    public void setFileUuid(String v)
    {
        fileUuid = v;
    }

    @Override
    public String getFileLocationUuid()
    {
        return fileLocationUuid;
    }

    @Override
    public void setFileLocationUuid(String v)
    {
        fileLocationUuid = v;
    }
}
