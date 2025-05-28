package com.teza.common.tardis.handlers;

/**
 * User: tom
 * Date: 10/10/16
 * Time: 2:26 PM
 */
public interface IndexRecord extends Comparable<IndexRecord>
{
    String getDocUuid();
    int getHierOrder();
    String getParentDocUuid();
    int getParentHierOrder();
    long getStartTs();
    long getEndTs();
    String getFileUuid();
    long getValidFromTs();
    long getValidToTs();
    String getContent();
    long getDataFromTs();
    long getDataToTs();
    String getFileMeta();
    String getLocationUuids();
    String[] splitLocationUuids();
}
