package com.teza.common.tardis.handlers;

import com.teza.common.tardis.Doc;
import com.teza.common.tardis.FileLocation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 7:01 PM
 */
public class IndexResult
{
    private final Map<String, Doc> docs;
    private final Map<String, FileLocation> locations;
    private final List<IndexRecord> indexRecords;

    public IndexResult()
    {
        docs = new HashMap<String, Doc>();
        locations = new HashMap<String, FileLocation>();
        indexRecords = new ArrayList<IndexRecord>();
    }

    private IndexResult(Map<String, Doc> docs, Map<String, FileLocation> locations,
                        List<IndexRecord> indexRecords)
    {
        this.docs = docs;
        this.locations = locations;
        this.indexRecords = indexRecords;
    }

    public void add(IndexRecord ir)
    {
        indexRecords.add(ir);
    }

    public void addDoc(Doc doc)
    {
        docs.put(doc.getDocUuid(), doc);
    }

    public void addLocation(FileLocation location)
    {
        locations.put(location.getFileLocationUuid(), location);
    }

    public Map<String, Doc> getDocs()
    {
        return docs;
    }

    public Map<String, FileLocation> getLocations()
    {
        return locations;
    }

    public List<IndexRecord> getIndexRecords()
    {
        return indexRecords;
    }

    public boolean isEmpty()
    {
        return indexRecords.isEmpty();
    }

    public IndexResult transform()
    {
        List<IndexRecord> impls = new ArrayList<IndexRecord>();
        for (IndexRecord r : indexRecords)
        {
            impls.add(new IndexRecordImpl(r));
        }
        return new IndexResult(docs, locations, impls);
    }
}
