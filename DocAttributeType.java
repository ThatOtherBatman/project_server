package com.teza.common.tardis;

/**
 * User: tom
 * Date: 4/6/17
 * Time: 5:36 PM
 */
public interface DocAttributeType
{
    int getDocAttributeTypeId();
    String getName();
    String getValueType();
    String getDescription();

    String cleanValue(String value);

    enum ValueType
    {
        UUID,
        STRING,
        INT,
        FLOAT,
        DATETIME,
        JSON,
        BOOL
    }
}
