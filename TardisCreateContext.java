package com.teza.common.tardis;

import com.teza.common.log.Log;
import org.joda.time.DateTime;

/**
 * User: paul
 * Date: 1/19/17
 * Time: 4:37 PM
 */
public interface TardisCreateContext
{
    /**
     * Retrieve data from Tardis
     * @param doc
     * @param start
     * @param end
     * @return
     */
    TardisResult getTardis(Doc doc, DateTime start, DateTime end);

    /**
     * Retrieve data from Tardis
     * @param doc
     * @param start
     * @param end
     * @param knowledge
     * @param freeze
     * @return
     */
    TardisResult getTardis(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze);

    Log getLog();
}
