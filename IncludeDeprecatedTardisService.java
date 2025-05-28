package com.teza.common.tardis;

import com.teza.common.tardis.deprecated.SearchResult;
import org.joda.time.DateTime;

/**
 * User: tom
 * Date: 1/6/17
 * Time: 1:33 PM
 */
public interface IncludeDeprecatedTardisService extends TardisService
{
    @Deprecated
    SearchResult lookAhead(String docUUID, DateTime start, DateTime end, DateTime knowledge);

    @Deprecated
    SearchResult lookBehind(String docUUID, DateTime start, DateTime end, DateTime knowledge);
}
