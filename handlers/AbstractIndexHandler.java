package com.teza.common.tardis.handlers;

import org.joda.time.DateTime;

import java.util.*;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 5:14 PM
 */
abstract class AbstractIndexHandler implements IndexHandler
{
    @Override
    public HandlerResult handle(String docUuid, DateTime startTs, DateTime endTs,
                                DateTime knowledgeTs, DateTime freezeTs,
                                List<IndexRecord> indexRecords)
    {
        if (indexRecords == null || indexRecords.isEmpty())
        {
            return null;
        }

        Map<String, HierarchyNode> hier = getHierarchy(indexRecords);
        List<Range> missing = new ArrayList<Range>();
        missing.add(new Range(startTs.getMillis(), endTs.getMillis()));
        String rootKey = docUuid + 0;
        return handle(hier, rootKey, missing, knowledgeTs.getMillis(), freezeTs.getMillis());
    }

    abstract HandlerResult handle(Map<String, HierarchyNode> hier, String key,
                                  List<Range> missing, long knowledgeTs,
                                  long freezeTs);

    private Map<String, HierarchyNode> getHierarchy(List<IndexRecord> indexRecords)
    {
        Collections.sort(indexRecords);
        Map<String, HierarchyNode> hier = new HashMap<String, HierarchyNode>();
        String key, parentKey;
        HierarchyNode node;
        for (IndexRecord ir : indexRecords)
        {
            key = ir.getDocUuid() + ir.getHierOrder();
            parentKey = ir.getParentDocUuid() + ir.getParentHierOrder();
            if (!hier.containsKey(key))
            {
                hier.put(key, new HierarchyNode(key));
            }
            node = hier.get(key);
            node.addFile(ir);
            if (hier.containsKey(parentKey))
            {
                hier.get(parentKey).addChild(ir.getHierOrder(), node);
            }
        }
        return hier;
    }

    protected HandlerResult getTimeSeries(Map<String, HierarchyNode> hier,
                                          String key, List<Range> missing,
                                          long knowledgeTs)
    {
        HandlerResult result = new HandlerResult(missing);
        if (!result.hasMissing())
        {
            return result;
        }

        HierarchyNode node = hier.get(key);
        TreeMap<Long, List<IndexRecord>> indexRecordsByKts = node.getIndexRecordsByKts();
        HandlerResult r;
        for (Long kts : indexRecordsByKts.descendingKeySet().tailSet(knowledgeTs, true))
        {
            r = buildTimeSeries(result.getMissing(), indexRecordsByKts.get(kts));
            result.merge(r);
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
                        r = getTimeSeries(hier, n.getKey(), result.getMissing(), knowledgeTs);
                        result.merge(r);
                    }
                }
                else
                {
                    List<HandlerResult> results = new ArrayList<HandlerResult>();
                    List<Range> intersection = new ArrayList<Range>();
                    intersection.addAll(result.getMissing());
                    for (HierarchyNode n : nodes)
                    {
                        r = getTimeSeries(hier, n.getKey(), intersection, knowledgeTs);
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
                    if (intersection.size() > 0)
                    {
                        r = mergeResults(result.getMissing(), results);
                        result.merge(r);
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

    protected HandlerResult buildTimeSeries(List<Range> missing, List<IndexRecord> files)
    {
        int n = files.size();
        if (missing.size() == 0 || n == 0)
        {
            return null;
        }
        int i = 0;
        List<Range> stillMissing = new ArrayList<Range>();
        List<Range> found = new ArrayList<Range>();
        for (Range m : missing)
        {
            while (i < n)
            {
                IndexRecord ir = files.get(i);
                if (ir.getStartTs() >= m.getEndTs())
                {
                    // file is after missing range
                    break;
                }
                else if (ir.getEndTs() <= m.getStartTs())
                {
                    // file is ahead of missing
                    i++;
                }
                else if (ir.getStartTs() <= m.getStartTs())
                {
                    // file starts before or same as missing
                    if (ir.getEndTs() < m.getEndTs())
                    {
                        // file starts before or during missing and ends before missing
                        found.add(new Range(m.getStartTs(), ir.getEndTs(), ir));
                        i++;
                        m = new Range(ir.getEndTs(), m.getEndTs());
                    }
                    else
                    {
                        // file full spans the missing range
                        found.add(new Range(m.getStartTs(), m.getEndTs(), ir));
                        m = null;
                        break;
                    }
                }
                else
                {
                    // file start after missing
                    stillMissing.add(new Range(m.getStartTs(), ir.getStartTs()));
                    if (ir.getEndTs() < m.getEndTs())
                    {
                        // file starts after missing and ends before missing
                        found.add(new Range(ir.getStartTs(), ir.getEndTs(), ir));
                        m = new Range(ir.getEndTs(), m.getEndTs());
                        i++;
                    }
                    else
                    {
                        // file starts after missing but ends on or after missing
                        found.add(new Range(ir.getStartTs(), m.getEndTs(), ir));
                        m = null;
                        break;
                    }
                }
            }
            if (m != null)
            {
                stillMissing.add(m);
            }
        }
        return new HandlerResult(stillMissing, found);
    }

    HandlerResult mergeResults(List<Range> missing, List<HandlerResult> results)
    {
        ListIterator<HandlerResult> li = results.listIterator(results.size());
        if (li.hasPrevious())
        {
            List<Range> found = li.previous().getFound();
            while (li.hasPrevious())
            {
                found = getIntersection(found, li.previous().getFound());
                if (found.size() == 0)
                {
                    break;
                }
            }
            List<Range> stillMissing = getLeftOnly(missing, found);
            return new HandlerResult(stillMissing, found);
        }
        return null;
    }

    private List<Range> getIntersection(List<Range> left, List<Range> right)
    {
        List<Range> found = new ArrayList<Range>();
        int n = right.size();
        if (left.size() == 0 || n == 0)
        {
            return found;
        }
        int i = 0;
        for (Range l : left)
        {
            while (i < n)
            {
                Range r = right.get(i);
                if (r.getStartTs() >= l.getEndTs())
                {
                    // right range is after left range
                    break;
                }
                else if (r.getEndTs() <= l.getStartTs())
                {
                    // right is ahead of left
                    i++;
                }
                else if (r.getStartTs() <= l.getStartTs())
                {
                    // right starts before or same as left
                    if (r.getEndTs() < l.getEndTs())
                    {
                        // right starts before or during left, and ends before left
                        found.add(new Range(l.getStartTs(), r.getEndTs(),
                                l.getIndexRecords(), r.getIndexRecords()));
                        i++;
                        l = new Range(r.getEndTs(), l.getEndTs(), l.getIndexRecords());
                    }
                    else
                    {
                        // right full spans the left range
                        found.add(new Range(l.getStartTs(), l.getEndTs(),
                                l.getIndexRecords(), r.getIndexRecords()));
                        break;
                    }
                }
                else
                {
                    // right start after left, still overlapping
                    if (r.getEndTs() < l.getEndTs())
                    {
                        // right starts after left, and ends before left
                        found.add(new Range(r.getStartTs(), r.getEndTs(),
                                l.getIndexRecords(), r.getIndexRecords()));
                        l = new Range(r.getEndTs(), l.getEndTs(), l.getIndexRecords());
                        i++;
                    }
                    else
                    {
                        // right starts after missing but ends on or after left
                        found.add(new Range(r.getStartTs(), l.getEndTs(),
                                l.getIndexRecords(), r.getIndexRecords()));
                        break;
                    }
                }
            }
        }
        return found;
    }

    List<Range> getLeftOnly(List<Range> left, List<Range> right)
    {
        int n = right.size();
        List<Range> leftOnly = new ArrayList<Range>();
        if (left.size() == 0)
        {
            return leftOnly;
        }
        else if (n == 0)
        {
            return left;
        }
        int i = 0;
        for (Range l : left)
        {
            while (i < n)
            {
                Range r = right.get(i);
                if (r.getStartTs() >= l.getEndTs())
                {
                    // right is after left range
                    break;
                }
                else if (r.getEndTs() <= l.getStartTs())
                {
                    // right is ahead of left
                    i++;
                }
                else if (r.getStartTs() <= l.getStartTs())
                {
                    // right starts before or same as left
                    if (r.getEndTs() < l.getEndTs())
                    {
                        // right starts before or during left and ends before left
                        i++;
                        l = new Range(r.getEndTs(), l.getEndTs());
                    }
                    else
                    {
                        // file full spans the missing range
                        l = null;
                        break;
                    }
                }
                else
                {
                    // right start after left
                    leftOnly.add(new Range(l.getStartTs(), r.getStartTs()));
                    if (r.getEndTs() < l.getEndTs())
                    {
                        // right starts after left and ends before left
                        l = new Range(r.getEndTs(), l.getEndTs());
                        i++;
                    }
                    else
                    {
                        // right starts after left but ends on or after left
                        l = null;
                        break;
                    }
                }
            }
            if (l != null)
            {
                leftOnly.add(l);
            }
        }
        return leftOnly;
    }

    static class HierarchyNode
    {
        private final TreeMap<Long, List<IndexRecord>> indexRecordsByKts = new TreeMap<Long, List<IndexRecord>>();
        private final TreeMap<Integer, Set<HierarchyNode>> edges = new TreeMap<Integer, Set<HierarchyNode>>();
        private final String key;

        HierarchyNode(String key)
        {
            this.key = key;
        }

        public void addChild(Integer hierOrder, HierarchyNode child)
        {
            if (!edges.containsKey(hierOrder))
            {
                edges.put(hierOrder, new HashSet<HierarchyNode>());
            }
            edges.get(hierOrder).add(child);
        }

        public void addFile(IndexRecord ir)
        {
            if (ir.getFileUuid() == null)
                return;

            Long kts = ir.getValidFromTs();
            if (!indexRecordsByKts.containsKey(kts))
            {
                indexRecordsByKts.put(kts, new ArrayList<IndexRecord>());
            }
            indexRecordsByKts.get(kts).add(ir);
        }

        public String getKey()
        {
            return key;
        }

        public TreeMap<Long, List<IndexRecord>> getIndexRecordsByKts()
        {
            return indexRecordsByKts;
        }

        public TreeMap<Integer, Set<HierarchyNode>> getEdges()
        {
            return edges;
        }
    }
}
