package com.teza.common.tardis.handlers;

import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 4:49 PM
 */
public class LookBehindHandler extends AbstractIndexHandler
{
    @Override
    HandlerResult handle(Map<String, HierarchyNode> hier,
                         String key, List<Range> missing,
                         long knowledgeTs, long freezeTs)
    {
        if (freezeTs < knowledgeTs)
        {
            throw new IllegalArgumentException("freezeTs " + freezeTs + " is less than knowledgeTs " + knowledgeTs);
        }
        HandlerResult result = getTimeSeries(hier, key, missing, knowledgeTs);
        result.processCleared();
        result.sort();
        return result;
    }
}
