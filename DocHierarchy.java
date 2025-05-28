package com.teza.common.tardis;

import org.joda.time.DateTime;

/**
 * User: tom
 * Date: 2/7/17
 * Time: 4:18 PM
 */
public interface DocHierarchy
{
    String getParentDocUuid();
    String getDocUuid();
    DateTime getDataFromTs();
    DateTime getDataToTs();
    int getHierOrder();
    String getClientUuid();

    void setParentDocUuid(String value);
    void setDocUuid(String value);
    void setDataFromTs(DateTime value);
    void setDataToTs(DateTime value);
    void setHierOrder(int value);
    void setClientUuid(String value);
}
