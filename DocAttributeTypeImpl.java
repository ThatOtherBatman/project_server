package com.teza.common.tardis;

import org.json.JSONObject;

/**
 * User: tom
 * Date: 4/6/17
 * Time: 5:38 PM
 */
public class DocAttributeTypeImpl implements DocAttributeType
{
    private int typeId;
    private String name, description;
    private DocAttributeType.ValueType valueType;

    public DocAttributeTypeImpl(int typeId, String name, String valueType)
    {
        this(typeId, name, valueType, null);
    }

    public DocAttributeTypeImpl(int typeId, String name, String valueType, String description)
    {
        this.typeId = typeId;
        this.name = name;
        this.valueType = DocAttributeType.ValueType.valueOf(valueType);
        this.description = description;
    }

    @Override
    public int getDocAttributeTypeId()
    {
        return typeId;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getValueType()
    {
        return valueType.toString();
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public String cleanValue(String value)
    {
        if (value == null)
        {
            throw new RuntimeException("value cannot be empty");
        }
        if (value.equals(TardisUtils.DELETED_ATTRIBUTE_VALUE))
        {
            return value;
        }

        switch(valueType)
        {
            case STRING:
                return value;
            case UUID:
                return UUID5.fromString(value).toString();
            case INT:
                return Integer.toString(Integer.parseInt(value));
            case FLOAT:
                return Double.toString(Double.parseDouble(value));
            case DATETIME:
                return TardisUtils.formatDateTime(TardisUtils.parseDateTime(value));
            case JSON:
                new JSONObject(value);
                return value;
            case BOOL:
                if (value.length() > 5)
                    throw new RuntimeException("invalid boolean value: " + value);

                if (value.toLowerCase().equals("true"))
                    return "True";
                else if (value.toLowerCase().equals("false"))
                    return "False";
                throw new RuntimeException("invalid boolean value: " + value);
            default:
                throw new RuntimeException("invalid value type " + valueType);
        }
    }

}
