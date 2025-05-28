package com.teza.common.tardis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 2/7/17
 * Time: 8:19 PM
 */
public class DocHierarchyResult
{
    private final Map<String, Doc> docs = new HashMap<String, Doc>();
    private final List<DocHierarchy> rows = new ArrayList<DocHierarchy>();

    void addDocHierarchy(DocHierarchy dh)
    {
        rows.add(dh);
    }

    void addDoc(Doc doc)
    {
        docs.put(doc.getDocUuid(), doc);
    }

    public List<DocHierarchy> getRows()
    {
        return rows;
    }

    public Map<String, Doc> getDocs()
    {
        return docs;
    }
}
