package com.teza.common.tardis;

/**
 * User: tom
 * Date: 1/4/17
 * Time: 3:46 PM
 */

/**
 *
 * Enum of the two currently supported modes of retrieving data with respect to the knowledge DateTime.
 *     - Method.LOOK_BEHIND:
 *         this method of data retrieval will return the latest data
 *         indexed into Tardis on or prior to the specified "knowledge" DateTime
 *     - Method.LOOK_AHEAD:
 *         LOOK_AHEAD result will always include everything from Method.LOOK_BEHIND.
 *         In the case that the requested "end" DateTime is greater than the latest
 *         data indexed prior to "knowledge" DateTime, this method will also include
 *         the *first* version of data indexed *after* the specified "knowledge" DateTime.
 *
 *         for instance, if you request data with a start and end range of
 *             ["2017-01-01 00:00:00 UTC", "2017-01-31 00:00:00 UTC")
 *         with a knowledge time of "2017-01-15 00:00:00 UTC", and in Tardis,
 *         the following were indexed:
 *             data #1 spanning ["2017-01-01 00:00:00 UTC", "2017-01-14 00:00:00 UTC") indexed at "2017-01-14 19:00:00 UTC"
 *             data #2 spanning ["2017-01-10 00:00:00 UTC", "2017-01-20 00:00:00 UTC") indexed at "2017-01-20 19:00:00 UTC"
 *             data #3 spanning ["2017-01-14 00:00:00 UTC", "2017-02-01 00:00:00 UTC") indexed at "2017-01-31 19:00:00 UTC"
 *
 *         the LOOK_BEHIND would only return data #1, because that's the only data
 *         that was indexed on or prior to the specified knowledge date of "2017-01-15 00:00:00 UTC"
 *
 *         the LOOK_AHEAD would see that the results only spanned up to "2017-01-14 00:00:00 UTC",
 *         and would also return the first version of data beyond "2017-01-14 00:00:00 UTC":
 *             the slice of ["2017-01-14 00:00:00 UTC", "2017-01-20 00:00:00 UTC") of data #2
 *             the slice of ["2017-01-20 00:00:00 UTC", "2017-01-31 00:00:00 UTC") of data #3
 */
public enum Method
{
    LOOK_BEHIND,
    LOOK_AHEAD,
    LAST
}
