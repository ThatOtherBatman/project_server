package com.teza.common.tardis;

import org.joda.time.DateTime;

/**
 * User: tom
 * Date: 2/7/17
 * Time: 4:25 PM
 */
public class DocHierarchyImpl implements DocHierarchy
{
    private String parentDocUuid, docUuid, clientUuid;
    private DateTime dataFromTs, dataToTs;
    private int hierOrder;

    public DocHierarchyImpl(String parentDocUuid, String docUuid, DateTime dataFromTs, DateTime dataToTs, int hierOrder, String clientUuid)
    {
        this.parentDocUuid= parentDocUuid;
        this.docUuid = docUuid;
        this.dataFromTs = dataFromTs;
        this.dataToTs = dataToTs;
        this.hierOrder = hierOrder;
        this.clientUuid = clientUuid;
    }

    @Override
    public String getParentDocUuid()
    {
        return parentDocUuid;
    }

    @Override
    public String getDocUuid()
    {
        return docUuid;
    }

    @Override
    public DateTime getDataFromTs()
    {
        return dataFromTs;
    }

    @Override
    public DateTime getDataToTs()
    {
        return dataToTs;
    }

    @Override
    public int getHierOrder()
    {
        return hierOrder;
    }

    @Override
    public String getClientUuid()
    {
        return clientUuid;
    }

    @Override
    public void setParentDocUuid(String value)
    {
        parentDocUuid = value;
    }

    @Override
    public void setDocUuid(String value)
    {
        docUuid = value;
    }

    @Override
    public void setDataFromTs(DateTime value)
    {
        dataFromTs = value;
    }

    @Override
    public void setDataToTs(DateTime value)
    {
        dataToTs = value;
    }

    @Override
    public void setHierOrder(int value)
    {
        hierOrder = value;
    }

    @Override
    public void setClientUuid(String value)
    {
        clientUuid = value;
    }

    public static String getUniqueKey(DocHierarchy dh)
    {
        return dh.getParentDocUuid() + "," + dh.getDocUuid() + "," +
                dh.getDataFromTs().getMillis() + "," +
                dh.getDataToTs().getMillis() + "," + dh.getHierOrder();
//        int hashCode = 0;
//        long value;
//        hashCode = (hashCode << 1) + (hashCode << 2) + (hashCode << 3) + (hashCode << 4) + dh.getParentDocUuid().hashCode();
//        hashCode += (hashCode << 1) + (hashCode << 2) + (hashCode << 3) + (hashCode << 4) + dh.getDocUuid().hashCode();
//        value = dh.getDataFromTs().getMillis();
//        hashCode += (hashCode << 1) + (hashCode << 2) + (hashCode << 3) + (hashCode << 4) + (int)(value ^ (value >> 32));
//        value = dh.getDataToTs().getMillis();
//        hashCode += (hashCode << 1) + (hashCode << 2) + (hashCode << 3) + (hashCode << 4) + (int)(value ^ (value >> 32));
//        hashCode += (hashCode << 1) + (hashCode << 2) + (hashCode << 3) + (hashCode << 4) + dh.getHierOrder();
//        return hashCode;
    }
}
