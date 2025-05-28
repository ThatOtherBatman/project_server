package com.teza.common.tardis.handlers;

/**
 * User: tom
 * Date: 10/10/16
 * Time: 2:26 PM
 */
public class IndexRecordImpl implements IndexRecord
{
    private String docUuid, parentDocUuid, fileUuid, content, fileMeta, locationUuids;
    private int hierOrder, parentHierOrder;
    private long startTs, endTs, validFromTs, validToTs, dataFromTs, dataToTs;

    public IndexRecordImpl(String docUuid, int hierOrder,
                           String parentDocUuid, int parentHierOrder,
                           long startTs, long endTs, String fileUuid,
                           long validFromTs, long validToTs,
                           String content, long dataFromTs, long dataToTs,
                           String fileMeta, String locationUuids)
    {
        this.docUuid = docUuid;
        this.hierOrder = hierOrder;
        this.parentDocUuid = parentDocUuid;
        this.parentHierOrder = parentHierOrder;
        if (fileUuid != null)
        {
            startTs = startTs < dataFromTs ? dataFromTs : startTs;
            endTs = endTs > dataToTs ? dataToTs : endTs;
        }
        this.startTs = startTs;
        this.endTs = endTs;
        this.fileUuid = fileUuid;
        this.validFromTs = validFromTs;
        this.validToTs = validToTs;
        this.content = content;
        this.dataFromTs = dataFromTs;
        this.dataToTs = dataToTs;
        this.fileMeta = fileMeta;
        this.locationUuids = locationUuids;
    }

    IndexRecordImpl(IndexRecord ir)
    {
        docUuid = ir.getDocUuid();
        hierOrder = ir.getHierOrder();
        parentDocUuid = ir.getParentDocUuid();
        parentHierOrder = ir.getParentHierOrder();
        startTs = ir.getStartTs();
        endTs = ir.getEndTs();
        fileUuid = ir.getFileUuid();
        validFromTs = ir.getValidFromTs();
        validToTs = ir.getValidToTs();
        content = ir.getContent();
        dataFromTs = ir.getDataFromTs();
        dataToTs = ir.getDataToTs();
        fileMeta = ir.getFileMeta();
        locationUuids = ir.getLocationUuids();
    }

    public String getDocUuid()
    {
        return docUuid;
    }

    public int getHierOrder()
    {
        return hierOrder;
    }

    public String getParentDocUuid()
    {
        return parentDocUuid;
    }

    public int getParentHierOrder()
    {
        return parentHierOrder;
    }

    public long getStartTs()
    {
        return startTs;
    }

    public long getEndTs()
    {
        return endTs;
    }

    public String getFileUuid()
    {
        return fileUuid;
    }

    public long getValidFromTs()
    {
        return validFromTs;
    }

    public long getValidToTs()
    {
        return validToTs;
    }

    public String getContent()
    {
        return content;
    }

    public long getDataFromTs()
    {
        return dataFromTs;
    }

    public long getDataToTs()
    {
        return dataToTs;
    }

    public String getFileMeta()
    {
        return fileMeta;
    }

    public String getLocationUuids()
    {
        return locationUuids;
    }

    public String[] splitLocationUuids()
    {
        if (locationUuids == null)
        {
            return null;
        }
        return locationUuids.substring(1, locationUuids.length() - 1).split(",");
    }

    @Override
    public int compareTo(IndexRecord o)
    {
        int check = this.hierOrder < o.getHierOrder() ? -1 : (
                this.hierOrder > o.getHierOrder() ? 1 : 0);
        if (check != 0)
        {
            return check;
        }

        if (fileUuid == null)
        {
            if (o.getFileUuid() == null)
            {
                return this.startTs < o.getStartTs() ? -1 : (
                        this.startTs > o.getStartTs() ? 1 : 0);
            }
            return -1;
        }
        else if (o.getFileUuid() == null)
        {
            return 1;
        }

        check = this.validFromTs < o.getValidFromTs() ? -1 : (
                this.validFromTs > o.getValidFromTs() ? 1 : 0);
        if (check != 0)
        {
            return check;
        }

        check = this.startTs < o.getStartTs() ? -1 : (
                this.startTs > o.getStartTs() ? 1 : 0);
        if (check != 0)
        {
            return check;
        }

        // prefer longer ranges over shorter ranges
        return this.endTs > o.getEndTs() ? 1 : (
                this.endTs < o.getEndTs() ? -1 : 0);
    }
}
