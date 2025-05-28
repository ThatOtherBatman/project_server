package com.teza.common.tardis;

/**
 * User: tom
 * Date: 4/6/17
 * Time: 6:46 PM
 */
public interface DocAttribute
{
    String getDocUuid(); void setDocUuid(String docUuid);
    String getName(); void setName(String name);
    String getValue(); void setValue(String value);
    long getLastUpdatedMs(); void setLastUpdatedMs(long lastUpdatedMs);
}
