package com.teza.common.tardis.handlers;

import com.teza.common.util.TypeConverterUtils;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 4:50 PM
 */

/**
 * simple class wrapping around:
 * - an inclusive start DateTime
 * - an exclusive end DateTime
 * - a nullable list of Tardis files, each file index record represented by DateTimeIndexRecord
 */
public class DateTimeRange
{
    protected final DateTime startTs, endTs;
    private final List<DateTimeIndexRecord> indexRecords;

    DateTimeRange(Range r)
    {
        this(r.getStartTs(), r.getEndTs(), r.getIndexRecords());
    }

    private DateTimeRange(long startTs, long endTs, List<IndexRecord> indexRecords)
    {
        this.startTs = TypeConverterUtils.millisToDateTime(startTs);
        this.endTs = TypeConverterUtils.millisToDateTime(endTs);
        if (startTs >= endTs)
        {
            throw new RuntimeException("start " + this.startTs + " is the same as or after end " + this.endTs);
        }
        List<DateTimeIndexRecord> records;
        if (indexRecords == null)
        {
            records = null;
        }
        else
        {
            records = new ArrayList<DateTimeIndexRecord>();
            for (IndexRecord ir : indexRecords)
            {
                records.add(new DateTimeIndexRecord(ir));
            }
        }
        this.indexRecords = records;
    }

    public DateTime getStartTs()
    {
        return startTs;
    }

    public DateTime getEndTs()
    {
        return endTs;
    }

    public List<DateTimeIndexRecord> getIndexRecords()
    {
        return indexRecords;
    }

    public String toString()
    {
        if (indexRecords == null)
            return "Range[" + startTs + ", " + endTs + ")";
        StringBuilder sb = new StringBuilder()
                .append("\n    Range[").append(startTs).append(", ")
                .append(endTs).append("):\n");
        for (DateTimeIndexRecord r : indexRecords)
        {
            sb.append("        ").append(r);
        }
        return sb.toString();
    }

    public static class DateTimeIndexRecord
    {
        private final IndexRecord indexRecord;
        public DateTimeIndexRecord(IndexRecord ir)
        {
            indexRecord = ir;
        }

        public String getDocUuid()
        {
            return indexRecord.getDocUuid();
        }

        public int getHierOrder()
        {
            return indexRecord.getHierOrder();
        }

        public String getParentDocUuid()
        {
            return indexRecord.getParentDocUuid();
        }

        public int getParentHierOrder()
        {
            return indexRecord.getParentHierOrder();
        }

        public String getFileUuid()
        {
            return indexRecord.getFileUuid();
        }

        public String getContent()
        {
            return indexRecord.getContent();
        }

        public String getFileMeta()
        {
            return indexRecord.getFileMeta();
        }

        public DateTime getStartTs()
        {
            return TypeConverterUtils.millisToDateTime(indexRecord.getStartTs());
        }

        public DateTime getEndTs()
        {
            return TypeConverterUtils.millisToDateTime(indexRecord.getEndTs());
        }

        public DateTime getValidFromTs()
        {
            return TypeConverterUtils.millisToDateTime(indexRecord.getValidFromTs());
        }

        public DateTime getValidToTs()
        {
            return TypeConverterUtils.millisToDateTime(indexRecord.getValidToTs());
        }

        public DateTime getDataFromTs()
        {
            return TypeConverterUtils.millisToDateTime(indexRecord.getDataFromTs());
        }

        public DateTime getDataToTs()
        {
            return TypeConverterUtils.millisToDateTime(indexRecord.getDataToTs());
        }

        public String[] getLocationUuids()
        {
            return indexRecord.splitLocationUuids();
        }

        public boolean isEmpty()
        {
            return indexRecord.getFileUuid() == null;
        }

        public String toString()
        {
            return "Index[" + getStartTs() + ", " + getEndTs() + "): " + getFileMeta();
        }

        public IndexRecord getIndexRecord()
        {
            return indexRecord;
        }
    }
}
