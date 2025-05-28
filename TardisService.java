package com.teza.common.tardis;

import com.teza.common.Disposable;
import com.teza.common.tardis.handlers.IndexResult;
import org.joda.time.DateTime;

import java.util.List;

/**
 * User: tom
 * Date: 1/6/17
 * Time: 11:37 AM
 */
public interface TardisService extends TardisClient, Disposable
{
    /**
     * dispose service and free any resources it used
     */
    void dispose();

    /**
     * function that index a file into Tardis
     *
     * @param fileUUID: unique file UUID
     * @param fileLocationUuid: the location UUID that the file is indexed to
     * @param clientUUID: the client UUID of the user that indexed the file
     * @param docUUID: the doc UUID to index the file under
     * @param hash: the md5 hash of the file
     * @param start: the inclusive data start timestamp of the file
     * @param end: the exclusive data end timestamp of the file
     * @param knowledgeTime: the time that the file is known to Tardis
     * @param fileMeta: additional meta data about the file, used for information only
     */
    void indexFile(String fileUUID, String fileLocationUuid, String clientUUID, String docUUID, String hash, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force, boolean archive);

    /**
     * function that index content into Tardis
     *
     * @param clientUUID: the client UUID of the user that indexed the file
     * @param docUUID: the doc UUID to index the file under
     * @param content: the content to save
     * @param start: the inclusive data start timestamp of the file
     * @param end: the exclusive data end timestamp of the file
     * @param knowledgeTime: the time that the file is known to Tardis
     * @param fileMeta: additional meta data about the content, used for information only
     */
    void indexContent(String clientUUID, String docUUID, String content, DateTime start, DateTime end, DateTime knowledgeTime, String fileMeta, boolean force);

    /**
     * get the file indexing records from Tardis and other relevant info from Tardis
     *
     * @param docUUID: the doc UUID that the data is indexed under
     * @param start: the requested inclusive starting timestamp for the time-series data
     * @param end: the requested exclusive ending timestamp for the time-series data
     * @param knowledge: the specified knowledge timestamp for the range requested
     * @param method: the method of data retrieval
     * @return an index result object that incorporates missing ranges, found ranges of data,
     *         and other relevant information. it is never null.
     *         the IndexResult.isEmpty() is true iff the docUUID does not exist.
     */
    IndexResult getIndex(String docUUID, DateTime start, DateTime end, DateTime knowledge, Method method);

    /**
     * get the file indexing records corresponding to a specific doc UUID + file UUID
     * @param docUUID: the requested doc UUID
     * @param fileUUID: the requested file UUID
     * @return an index result object presenting the file UUID
     */
    IndexResult getIndex(String docUUID, String fileUUID);

    /**
     * get all the files corresponding to a specific doc UUID
     * @param docUUID: the requested doc UUID
     * @return an index result object presenting the file UUID
     */
    IndexResult getIndex(String docUUID);

    /**
     * create a tardis file location to place data
     * @param fileLocation: file location to create
     */
    void createLocation(FileLocation fileLocation, boolean force);

    /**
     * register an upload service
     * @param fileLocationUuid: the file location UUID to register as upload service
     * @param uploadUrl: the upload url corresponding to the file location
     * @param permittedSources: a comma delisted string of Source that are permitted,
     *                        if null, all sources are allowed
     */
    void addUploadLocation(String fileLocationUuid, String uploadUrl, String permittedSources);

    /**
     * deregister an upload service
     */
    void removeUploadLocation(String fileLocationUuid);

    /**
     * create attribute type
     * @param name: name of the attribute type
     * @param valueType: data type of the value
     * @param description: description of the attribute type
     */
    void createDocAttributeType(String name, String valueType, String description, String clientUuid);

    /**
     * delete a file index
     * @param docUuid: the doc UUID of the file to delete
     * @param fileUuid: the file UUID to delete
     * @param fileLocationUuid: the location to delete file from
     * @return whether or not the file can be deleted
     */
    String delete(String docUuid, String fileUuid, String fileLocationUuid);

    /**
     * soft-delete files (by moving to deleted_file_location_rel)
     *
     * @param docUuid: the doc UUID to soft-delete
     * @param archivePriority: delete only locations whose priority is LESS than archivePriority
     * @param uploadLimit: delete only if number of data centers for locations >= archivePriority
     *                   is >= this uploadLimit
     * @return true if there are files to delete, false otherwise
     */
    boolean softDeleteFiles(String docUuid, int archivePriority, int uploadLimit);

    /**
     * soft-delete all files for a doc
     * @return true if there are files to delete, false otherwise
     */
    boolean softDeleteFiles(String docUuid, String clientUuid);

    /**
     * UN-soft-delete all files for a doc
     */
    boolean unSoftDeleteFiles(String docUuid, String clientUuid);

    /**
     * get the list of soft-deleted files for a doc
     */
    List<FileLocationRel> getSoftDeletedFiles(String docUuid);

    /**
     * delete all records of a doc
     * @return true if successful, false otherwise
     */
    boolean purgeDoc(String docUuid);

    /**
     * Log access of docs by client
     * @param docUuid: The doc uuid of the doc being rad/tracked
     * @param clientUuid: The client uuid reading the doc
     */
    void logAccess(String docUuid, String clientUuid);
}
