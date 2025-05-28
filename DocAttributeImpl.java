package com.teza.common.tardis;

/**
 * User: tom
 * Date: 4/6/17
 * Time: 6:49 PM
 */
public class DocAttributeImpl implements DocAttribute
{
    private String docUuid;
    private String name;
    private String value;
    private long lastUpdatedMs;

    public DocAttributeImpl(String docUuid, String name, String value, long lastUpdatedMs)
    {
        this.docUuid = docUuid;
        this.name = name;
        this.value = value;
        this.lastUpdatedMs = lastUpdatedMs;
    }

    @Override
    public String getDocUuid()
    {
        return docUuid;
    }

    @Override
    public void setDocUuid(String docUuid)
    {
        this.docUuid = docUuid;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public String getValue()
    {
        return value;
    }

    @Override
    public void setValue(String value)
    {
        this.value = value;
    }

    @Override
    public long getLastUpdatedMs()
    {
        return lastUpdatedMs;
    }

    @Override
    public void setLastUpdatedMs(long lastUpdatedMs)
    {
        this.lastUpdatedMs = lastUpdatedMs;
    }
}
