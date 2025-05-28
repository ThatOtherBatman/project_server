package com.teza.common.tardis.deprecated;

/**
 * User: tom
 * Date: 10/10/16
 * Time: 2:26 PM
 */
public interface FileIndex
{
    String getDocUuid();
    String getHierOrder();
    String getParentDocUuid();
    String getParentHierOrder();
    String getStartTs();
    String getEndTs();
    String getFileUuid();
    String getDataFromTs();
    String getDataToTs();
    String getContent();
    String getValidFromTs();
    String getValidToTs();
    String getFileMeta();
    String getLocationUuids();
}
