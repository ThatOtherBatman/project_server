package com.teza.common.tardis;

/**
 * User: robin
 * Date: 9/19/16
 * Time: 9:31 AM
 */

/**
 * all data in tardis is indexed under a specific doc.
 * each doc has its own UUID identifier based on its properties.
 *
 * - the source is used to tag the doc with a particular category of data, see Source.java
 * - the doc describes how the data is serialized (getDataCls(), getDataClsVersion())
 * - getFilePattern() specifies the file name pattern that the serialized data is saved with
 * - getEnv() will be used in the future as a permission group to determine who can read/write to a doc
 * - getUniqueKeys() returns a json string that represents additional attributes of a doc
 * - getClientUuid() specifies who created the doc
 */
public interface Doc
{
    String getSource();
    String getDocUuid();
    String getDataCls();
    String getDataClsVersion();
    String getFilePattern();
    String getEnv();
    String getUniqueKeys();
    String getClientUuid();

    void setSource(String value);
    void setDocUuid(String value);
    void setDataCls(String value);
    void setDataClsVersion(String value);
    void setFilePattern(String value);
    void setEnv(String value);
    void setUniqueKeys(String value);
    void setClientUuid(String value);
}
