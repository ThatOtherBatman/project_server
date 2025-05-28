package com.teza.common.tardis;

import com.teza.common.tardis.datatypes.DataType;
import com.teza.common.tardis.datatypes.DataTypeFactory;
import com.teza.common.tardis.handlers.DateTimeRange;
import com.teza.common.tardis.handlers.HandlerResult;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 10:07 PM
 */

/**
 * An object that represents the result of getting data from Tardis
 *
 */
public class TardisResultImpl implements TardisResult
{

    private final HandlerResult handlerResult;
    private final Doc doc;
    private Map<String, Doc> docs = null;
    private Map<String, FileLocation> locations = null;
    private final LocationSelector locationSelector;

    public TardisResultImpl(Doc doc, HandlerResult handlerResult, Map<String, Doc> docs, Map<String, FileLocation> locations, LocationSelector locationSelector)
    {
        this.doc = doc;
        this.handlerResult = handlerResult;
        this.docs = docs;
        this.locations = locations;
        this.locationSelector = locationSelector;
    }

    /**
     * returns a list of DateTimeRange objects specifying
     * the DateTime ranges that are missing in Tardis based on initial request
     *
     * @return List of DateTimeRange objects
     */
    public List<DateTimeRange> getMissing()
    {
        return handlerResult.getMissingRanges();
    }

    /**
     * returns a sorted list of DateTimeRange objects specifying
     * the DateTime ranges and their corresponding data from Tardis
     *
     * @return List of DateTimeRange objects
     */
    public List<DateTimeRange> getFound()
    {
        return handlerResult.getFoundRanges();
    }

    @Override
    public boolean isEmpty()
    {
        return handlerResult.isEmpty();
    }

    @Override
    public String toString()
    {
        return "TardisResultImpl<" + doc.getDocUuid() + " missing: " + getMissing() + " found: " + getFound() + ">";
    }

    public DateTime getStartTs()
    {
        if (handlerResult.isEmpty())
        {
            return null;
        }
        return handlerResult.getFoundRanges().get(0).getStartTs();
    }

    public DateTime getEndTs()
    {
        if (handlerResult.isEmpty())
        {
            return null;
        }
        List<DateTimeRange> dtr = handlerResult.getFoundRanges();
        return dtr.get(dtr.size() - 1).getEndTs();
    }

    /**
     * for each data piece found in result, retrieve the data, and combine
     * the results into a final object.
     *
     * @param <T>: the expected datatype of the result
     * @return the result object from Tardis
     */
    public <T> T value()
    {
        if (handlerResult.isEmpty())
        {
            return null;
        }

        DataType<T> dataType = DataTypeFactory.getDataType(doc);
        List<T> results = new ArrayList<T>();
        T value;
        for (DateTimeRange fr : handlerResult)
        {
            List<T> rangeResults = new ArrayList<T>();
            for (DateTimeRange.DateTimeIndexRecord ir : fr.getIndexRecords())
            {
                // TODO: caching
                DateTime start = ir.getDataFromTs();
                DateTime end = ir.getDataToTs();
                Doc doc = docs.get(ir.getDocUuid());
                DataType<T> dt = DataTypeFactory.getDataType(doc.getDataCls(), doc.getDataClsVersion());
                if (ir.getFileUuid().equals(UUID5.NIL_UUID))
                {
                    value = dt.loadRecord(ir.getContent(), start, end, ir.getValidFromTs(), ir.getFileMeta());
                }
                else
                {
                    String fileName = TardisUtils.getFileName(doc, ir.getContent(), start, end);
                    List<FileLocation> fileLocations = new ArrayList<FileLocation>();
                    for (String locationUuid : ir.getLocationUuids())
                    {
                        fileLocations.add(locations.get(locationUuid));
                    }
                    FileLocation fileLocation = locationSelector.getBestLocation(fileLocations);
                    value = dt.loads(TardisUtils.getInputStream(fileName, fileLocation));
                }
                if (value != null)
                {
                    value = dt.getRange(value, fr.getStartTs(), fr.getEndTs(), start, end);
                    if (value != null)
                        rangeResults.add(value);
                }
            }
            value = dataType.join(rangeResults);
            if (value != null)
                results.add(value);
            rangeResults.clear();
        }
        return dataType.concat(results);
    }
}
