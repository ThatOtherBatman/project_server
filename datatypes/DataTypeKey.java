package com.teza.common.tardis.datatypes;

/**
 * User: tom
 * Date: 1/9/17
 * Time: 11:33 AM
 */
public interface DataTypeKey
{
    String getDataCls();
    String getDataClsVersion();
    DataTypeKey setDataCls(String dataCls);
    DataTypeKey setDataClsVersion(String dataClsVersion);
}
