package com.teza.common.tardis.handlers;

import java.util.*;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 4:58 PM
 */
public class LookAheadHandler extends LookBehindHandler
{
    @Override
    protected HandlerResult handle(Map<String, HierarchyNode> hier,
                                   String key, List<Range> missing,
                                   long knowledgeTs, long freezeTs)
    {
        HandlerResult result = super.handle(hier, key, missing, freezeTs, freezeTs);

        missing = getLookAheadMissing(result.getMissing(), result.getFound());
        if (missing.isEmpty())
        {
            return result;
        }

        HandlerResult r = getEarliest(hier, key, missing, knowledgeTs, freezeTs);
        if (!r.isEmpty())
        {
            List<Range> stillMissing = getLeftOnly(result.getMissing(), r.getFound());
            r = new HandlerResult(stillMissing, r.getFound());
            if (!r.processCleared() || !r.isEmpty())
            {
                result.merge(r);
            }
        }
        return result;
    }

    private List<Range> getLookAheadMissing(List<Range> missing, List<Range> found)
    {
        long start = 0;
        for (Range fr : found)
        {
            if (start == 0 || fr.getEndTs() > start)
            {
                start = fr.getEndTs();
            }
        }

        if (start == 0)
            return missing;

        List<Range> stillMissing = new ArrayList<Range>();
        for (Range m : missing)
        {
            if (m.getEndTs() <= start)
            {
                continue;
            }
            if (m.getStartTs() > start)
            {
                stillMissing.add(m);
            }
            else
            {
                stillMissing.add(new Range(start, m.getEndTs()));
                start = m.getEndTs();
            }
        }
        return stillMissing;
    }

    private HandlerResult getEarliest(Map<String, HierarchyNode> hier,
                                      String key, List<Range> missing,
                                      long knowledgeTs, long freezeTs)
    {
        HandlerResult result = new HandlerResult(missing);
        if (!result.hasMissing())
        {
            return result;
        }

        HierarchyNode node = hier.get(key);
        TreeMap<Long, List<IndexRecord>> indexRecordsByKts = node.getIndexRecordsByKts();
        HandlerResult r;
        for (Long kts : indexRecordsByKts.navigableKeySet().subSet(freezeTs, false, knowledgeTs, true))
        {
            r = buildTimeSeries(result.getMissing(), indexRecordsByKts.get(kts));
            if (!r.isEmpty())
            {
                missing = getLookAheadMissing(r.getMissing(), r.getFound());
                result.merge(new HandlerResult(missing, r.getFound()));
            }
            if (!result.hasMissing())
            {
                break;
            }
        }
        if (result.hasMissing())
        {
            TreeMap<Integer, Set<HierarchyNode>> edges = node.getEdges();
            for (Integer hierOrder : edges.keySet())
            {
                Set<HierarchyNode> nodes = edges.get(hierOrder);
                if (nodes.size() == 1)
                {
                    for (HierarchyNode n : nodes)
                    {
                        r = getEarliest(hier, n.getKey(), result.getMissing(), knowledgeTs, freezeTs);
                        if (!r.isEmpty())
                        {
                            missing = getLookAheadMissing(r.getMissing(), r.getFound());
                            result.merge(new HandlerResult(missing, r.getFound()));
                        }
                    }
                }
                else
                {
                    List<HandlerResult> results = new ArrayList<HandlerResult>();
                    List<Range> intersection = new ArrayList<Range>();
                    intersection.addAll(result.getMissing());
                    for (HierarchyNode n : nodes)
                    {
                        r = getEarliest(hier, n.getKey(), intersection, knowledgeTs, freezeTs);
                        if (r == null)
                        {
                            intersection.clear();
                            break;
                        }
                        else
                        {
                            r.sort();
                            intersection = r.getFound();
                            results.add(r);
                        }
                    }
                    if (!intersection.isEmpty())
                    {
                        r = mergeResults(result.getMissing(), results);
                        if (!r.isEmpty())
                        {
                            missing = getLookAheadMissing(r.getMissing(), r.getFound());
                            result.merge(new HandlerResult(missing, r.getFound()));
                        }
                    }
                }
                if (!result.hasMissing())
                {
                    break;
                }
            }
        }
        return result;
    }
}