package com.teza.common.tardis.handlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 9/22/17
 * Time: 5:16 PM
 */
public class LastHandler extends LookAheadHandler
{
    public static final List<Range> NOT_MISSING = new ArrayList<Range>();

    @Override
    protected HandlerResult handle(Map<String, HierarchyNode> hier,
                                   String key, List<Range> missing,
                                   long knowledgeTs, long freezeTs)
    {
        HandlerResult hr = super.handle(hier, key, missing, knowledgeTs, freezeTs);
        if (hr.isEmpty())
        {
            return hr;
        }
        List<Range> found = hr.getFound();
        List<Range> last = new ArrayList<Range>();
        last.add(found.get(found.size() - 1));
        return new HandlerResult(NOT_MISSING, last);
    }
}
