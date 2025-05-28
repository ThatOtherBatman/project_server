package com.teza.common.tardis;

import com.teza.common.tardis.handlers.IndexResult;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * User: robin
 * Date: 9/19/16
 * Time: 9:30 AM
 */

/**
 * At its core, Tardis is a way of storing time-series of data and their knowledge time.
 *
 * Each distinct time series is stored under a Doc, which is analogous to
 * a topic or a key in traditional key-value stores.
 * Doc is metadata about what is being indexed, including:
 * - the unique attributes that make this an unique key for time-series data
 * - how it should be serialized/deserialized
 * - how it is permissioned
 * - a UUID5 that present all of the above
 *
 * Tardis keeps track of who put data into the system with a Client instance,
 * which specifies user name, host name, teza environment and application name+version
 *
 * To ensure redundancy, data stored into Tardis is saved in multiple locations,
 * and the location is represented by FileLocation interface
 *
 */
public interface TardisClient
{
    boolean createDoc(Doc doc);
    Doc getDoc(String uuid);

    /**
     * search for all existing doc's whose key-values
     * satisfy all of the key-value pairs specified by query
     *
     * @param query: a dictionary of Doc attributes and their values
     * @return a list of Docs that satisify all key-value pairs specified by query
     */
    List<Doc> searchDocs(JSONObject query);

    List<String> getDistinctDocFields(JSONObject query, String field);

    List<DocAttribute> searchDocAttributes(JSONObject query, boolean history);


    Client getClient(String uuid);
    void createClient(Client client);

    FileLocation getLocation(String uuid, boolean force);

    /**
     * retrieve previously indexed data as a TardisResult
     *
     * @param doc: the Doc that the data is indexed under
     * @param start: the INCLUSIVE starting DateTime of the the data being requested
     * @param end: the EXCLUSIVE ending DateTime of the data being requested
     * @param knowledge: the knowledge DateTime, asking Tardis what data was available as of this time.
     * @param freeze: when the "freeze" DateTime is null, the get function returns the latest data
     *              indexed into Tardis on or prior to the specified "knowledge" DateTime
     *
     *              when the freeze DateTime is not null, the get funciton returns the latest data
     *              indexed into Tardis on or prior to the specifed "freeze" DateTime, as well as
     *              the *first* version of data indexed *after* the specified "freeze" DateTime
     *              but on or before the "knowledge" Datetime, whose
     *              "end" DateTime is greater than any data indexed on or prior to the "freeze" DateTime
     *
     *              for instance, if you request data with a start and end range of
     *                  ["2017-01-01 00:00:00 UTC", "2017-01-31 00:00:00 UTC")
     *              with a knowledge time of "2017-01-21 00:00:00 UTC", and the following were indexed in Tardis:
     *                  data #1 spanning ["2017-01-01 00:00:00 UTC", "2017-01-14 00:00:00 UTC") indexed at "2017-01-14 19:00:00 UTC"
     *                  data #2 spanning ["2017-01-10 00:00:00 UTC", "2017-01-20 00:00:00 UTC") indexed at "2017-01-20 19:00:00 UTC"
     *                  data #3 spanning ["2017-01-14 00:00:00 UTC", "2017-02-01 00:00:00 UTC") indexed at "2017-01-31 19:00:00 UTC"
     *
     *              freeze == null would return:
     *                  the partial slice of ["2017-01-01 00:00:00 UTC", "2017-01-10 00:00:00 UTC") of data #1
     *                  the full slice of ["2017-01-10 00:00:00 UTC", "2017-01-20 00:00:00 UTC") of data #2
     *              because that was the latest data indexed on or prior to the specified knowledge date of "2017-01-21 00:00:00 UTC"
     *
     *              freeze == "2017-01-15 00:00:00 UTC" would return:
     *                  the full slice of ["2017-01-01 00:00:00 UTC", "2017-01-14 00:00:00 UTC") of data #1
     *                      since it was the latest data indexed prior to the freeze
     *                  the partial slice of ["2017-01-14 00:00:00 UTC", "2017-01-20 00:00:00 UTC") of data #2
     *                      since it was the first version of data index after the freeze,
     *                      that exceeded the data #1's "end" DateTime of "2017-01-14 00:00:00 UTC"
     *
     *              if the freeze date is still "2017-01-15 00:00:00 UTC", and the knowledge date has moved to
     *              "2017-02-01 00:00:00 UTC",
     *              the LOOK_AHEAD would see that the results only spanned up to "2017-01-14 00:00:00 UTC",
     *              and would also return the first version of data beyond "2017-01-14 00:00:00 UTC":
     *                 the slice of ["2017-01-14 00:00:00 UTC", "2017-01-20 00:00:00 UTC") of data #2
     *                 the slice of ["2017-01-20 00:00:00 UTC", "2017-01-31 00:00:00 UTC") of data #3
     * @param force: specify true to ignore any (short lived 10-second) server-side caches of TardisResult (default: false)
     * @return a TardisResult object representing the results of the query
     */
    TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, boolean force);
    TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze);

    /**
     * build a tardis result given an existing index result
     * @param method: specify the method used to retrieve data, see Method for more details
     * @param ir: specify the existing index result
     */
    TardisResult get(Doc doc, DateTime start, DateTime end, DateTime knowledge, DateTime freeze, Method method, IndexResult ir, boolean force);

    /**
     * convenience method that calls .get with default knownledge and freeze
     */
    TardisResult get(Doc doc, DateTime start, DateTime end);

    /**
     * put an object into tardis
     * @param doc: doc to store data under
     * @param value: value to put into Tardis
     * @param start: the INCLUSIVE starting DateTime of the data being stored
     * @param end: the EXCLUSIVE ending DateTime of the data being stored
     * @param knowledge: the knowledge DateTime, specifying when the data was available
     */
    <T> void put(Doc doc, T value, DateTime start, DateTime end, DateTime knowledge);

    /**
     * put an object into tardis as the content field of the index record
     * this is for scalar value storage and others of a similar nature
     *
     * @param doc: doc to store under
     * @param value: SCALAR or tiny value to put into tardis
     * @param start: the INCLUSIVE starting DateTime of the data being stored
     * @param end: the EXCLUSIVE ending DateTime of the data being stored
     * @param knowledge: the knowledge DateTime, specifying when the data was available
     */
    void putContent(Doc doc, Object value, DateTime start, DateTime end, DateTime knowledge);


    /**
     * clears what's stored in tardis for a particular data time range,
     * as of a specific knowledge time
     *
     * @param doc: doc to clear
     * @param start: the INCLUSIVE starting DateTime of the data being stored
     * @param end: the EXCLUSIVE ending DateTime of the data being stored
     * @param knowledge: the knowledge DateTime, specifying when the data was available
     */
    void clear(Doc doc, DateTime start, DateTime end, DateTime knowledge);

    /**
     * get a list of upload services
     */
    List<UploadLocation> getUploadLocations();

    /**
     * create doc hierarchy (and sub hierarchies)
     */
    void createDocHierarchy(List<DocHierarchy> docHierarchies);

    /**
     * get doc hierarchy
     */
    DocHierarchyResult getDocHierarchy(String docUuid, DateTime knowledge);

    DocAttributeType getDocAttributeType(String name);

    /**
     * set an attribute of a doc
     * @param docUuid: doc's uuid
     * @param name: name of the attribute
     * @param value: value of the attribute
     * @return whether the attribute was updated
     */
    boolean setDocAttribute(String docUuid, String name, String value, String clientUuid);

    /**
     * clear an attribute of a doc
     * @param docUuid: doc's uuid
     * @param name: name of the attribute
     * @return whether the attribute was deleted
     */
    boolean clearDocAttribute(String docUuid, String name, String clientUuid);

    /**
     * get the value of an attribute of a doc
     * @param docUuid: doc's uuid
     * @param name: name of the attribute
     * @param knowledge: knowledge time, if null defaults to latest
     * @return attribute value
     */
    DocAttribute getDocAttribute(String docUuid, String name, DateTime knowledge);

    /**
     * get doc attributes
     * @param docUuid: doc's uuid
     * @param knowledge: knowledge time, if null defaults to latest
     */
    Map<String, String> getDocAttributes(String docUuid, DateTime knowledge);

    /**
     * get all attribute types
     * @return a dictionary of attribute type name to data type
     */
    List<DocAttributeType> getAttributeTypes();

    /**
     * see .get for detailed explanation of knowledge and freeze DateTime
     */
    DateTime getDefaultKnowledgeTs();
    DateTime getDefaultFreezeTs();

}
