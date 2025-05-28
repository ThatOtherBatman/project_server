package com.teza.common.tardis.datatypes;

import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * User: tom
 * Date: 1/9/17
 * Time: 11:26 AM
 */
public interface DataType<T>
{
    boolean isNull(T value);

    /**
     * performs any QC checks on "value" object based on data range
     * @param value: value to check/clean
     * @param start: expected INCLUSIVE starting DateTime of the value
     * @param end: expected EXCLUSIVE ending DateTime of the value
     * @return possibly a modified version of value based on expected date range
     */
    T clean(T value, DateTime start, DateTime end);

    /**
     * slice "value" object based on desired and actual datetime range of data
     * @param value: data to slice
     * @param start: desired INCLUSIVE starting DateTime
     * @param end: desired EXCLUSIVE ending DateTime
     * @param dataFromTs: actual INCLUSIVE DateTiem of "value"
     * @param dataToTs: actual EXCLUSIVE DateTime of "value"
     * @return a slice of "value" fitting the desired datetime range
     */
    T getRange(T value, DateTime start, DateTime end,
               DateTime dataFromTs, DateTime dataToTs);

    /**
     * join a list of values that correspond to the same date range
     * @param results: list of values
     * @return joined value also of the same date range
     */
    T join(List<T> results);

    /**
     * concat time sorted list of values that does not have overlapping date ranges
     * @param results: list of values
     * @return concatenated value spanning the min start to max end range
     */
    T concat(List<T> results);

    /**
     * build an object from an input stream
     * @param raw: the input stream of serialized object
     * @return corresponding object
     */
    T loads(InputStream raw);

    /**
     * dumps serialization of value into the specified output stream
     * @param value: value to dump
     * @param os: stream to dump to
     */
    void dumps(T value, OutputStream os);

    /**
     * get informational meta data about the value as file
     */
    JSONObject getMeta(T value);

    boolean isContent(Object value);

    Object cleanContent(Object value, DateTime start, DateTime end);

    /**
     * if the object is not serialized to file, but as the content column
     * in the database, convert all the data known at indexing time back
     * into a value
     *
     * @param content: special serialized string of the object to be a text column
     * @param start: INCLUSIVE starting DateTime corresponding to content
     * @param end: EXCLUSIVE ending DateTime corresponding to content
     * @param knowledge: knowledge time that the content was indexed with
     * @param fileMeta: special meta from getContentMeta(value)
     * @return the original value
     */
    T loadRecord(String content, DateTime start, DateTime end, DateTime knowledge, String fileMeta);

    /**
     * dump object into a string to be stored with index record
     * @param value: object to dump
     * @return serialized string to store
     */
    String dumpContent(Object value);

    /**
     * get informational meta data about the value as content
     */
    JSONObject getContentMeta(Object value);

    DataTypeKey[] getSupportedCodecs();
}
