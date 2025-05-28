package com.teza.common.tardis;

import com.teza.common.tardis.handlers.DateTimeRange;
import org.joda.time.DateTime;

import java.util.List;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 10:07 PM
 */

/**
 * An object that represents the result of getting data from Tardis
 *
 */
public interface TardisResult
{

    /**
     * returns a list of DateTimeRange objects specifying
     * the DateTime ranges that are missing in Tardis based on initial request
     *
     * @return List of DateTimeRange objects
     */
    List<DateTimeRange> getMissing();

    /**
     * returns a sorted list of DateTimeRange objects specifying
     * the DateTime ranges and their corresponding data from Tardis
     *
     * @return List of DateTimeRange objects
     */
    List<DateTimeRange> getFound();

    boolean isEmpty();

    DateTime getStartTs();
    DateTime getEndTs();

    /**
     * for each data piece found in result, retrieve the data, and combine
     * the results into a final object.
     *
     * @param <T>: the expected datatype of the result
     * @return the result object from Tardis
     */
    <T> T value();
}
