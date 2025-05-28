package com.teza.common.tardis.handlers;

import org.joda.time.DateTime;

import java.util.List;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 5:04 PM
 */
public interface IndexHandler
{
    /**
     * build a time-series of slices of index records, as well as a time-series of missing DateTime ranges
     *
     * @param docUuid: the requested Doc UUID corresponding to the IndexResult
     * @param startTs: the requested INCLUSIVE data range starting DateTime
     * @param endTs: the requested EXCLUSIVE data range ending DateTime
     * @param knowledgeTs: the requested knowledge DateTime
     * @param freezeTs: the requested freeze DateTime
     * @param indexRecords: the corresponding list of IndexRecord from TardisService
     * @return null iff the Doc does not exist, otherwise returns a HandlerResult object
     */
    HandlerResult handle(String docUuid, DateTime startTs, DateTime endTs, DateTime knowledgeTs, DateTime freezeTs, List<IndexRecord> indexRecords);
}
