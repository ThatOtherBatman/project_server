package com.teza.common.tardis.datatypes;

import com.teza.common.codegen.pojo.PojoFactory;
import com.teza.common.tardis.Doc;

import java.util.HashMap;
import java.util.Map;

/**
 * User: tom
 * Date: 1/9/17
 * Time: 11:22 AM
 */
public abstract class DataTypeFactory
{
    private static final Map<DataTypeKey, DataType> supported = new HashMap<DataTypeKey, DataType>();

    static
    {
        register(new ColumnSetDataType.TimestampInMicros());
        register(new ColumnSetDataType.TimestampInNanos());
        register(new LZ4TimestampInMicros());
        register(new ArbitraryFileDataType());
        register(new RawTimeSeriesDataType.Doubles());
        register(new LZ4SampledDataFile());
        register(new EventListJsonDataType());
    }

    public static void register(DataType instance)
    {
        register(instance.getSupportedCodecs(), instance);
    }

    static void register(DataTypeKey[] keys, DataType instance)
    {
        for (DataTypeKey k : keys)
        {
            if (supported.containsKey(k))
            {
                if (!supported.get(k).getClass().isInstance(instance))
                {
                    throw new RuntimeException("cannot reassign " + k + " from " + supported.get(k) + " to " + instance);
                }
            }
            supported.put(k, instance);
        }
    }

    public static DataTypeKey getKey(String dataCls, String dataClsVersion)
    {
        return PojoFactory.make(DataTypeKey.class).setDataCls(dataCls).setDataClsVersion(dataClsVersion);
    }

    public static <T> DataType<T> getDataType(Doc doc)
    {
        DataTypeKey key = getKey(doc.getDataCls(), doc.getDataClsVersion());
        if (supported.containsKey(key))
        {
            //noinspection unchecked
            return supported.get(key);
        }
        throw new RuntimeException("unsupported data type key " + key);
    }

    public static <T> DataType<T> getDataType(String dataCls, String dataClsVersion)
    {
        return getDataType(getKey(dataCls, dataClsVersion));
    }

    public static <T> DataType<T> getDataType(DataTypeKey key)
    {
        if (supported.containsKey(key))
        {
            //noinspection unchecked
            return supported.get(key);
        }
        throw new RuntimeException("unsupported data type key " + key);
    }

}
