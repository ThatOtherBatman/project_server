package com.teza.common.tardis.docs;

import com.teza.common.tardis.DocImpl;
import com.teza.common.tardis.Source;
import com.teza.common.tardis.TardisUtils;
import com.teza.common.tardis.datatypes.ArbitraryFileDataType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by xiangli on 1/26/17.
 */
public class ClearingOPFileManagerDoc  extends DocImpl
{
    private static final String filePattern =  "{source}/{vendor}/{category}_{from_date}_{to_date}_{content}.tardis";

    public ClearingOPFileManagerDoc(String broker, String fund, String category, List<String> features)
    {
        super(Source.GENERAL, new JSONObject(), filePattern, null, ArbitraryFileDataType.CODEC, "0");
        if (!TardisUtils.isValidFileNamePart(broker) ||
                !TardisUtils.isValidFileNamePart(fund) ||
                !TardisUtils.isValidFileNamePart(category) )
        {
            throw new IllegalArgumentException("invalid broker, category : (" +
                    broker + ", " + category +  ")");
        }


        JSONObject jsonUniqueKeys = new JSONObject()
                .put("name","BrokerReport")
                .put("category", category)
                .put("vendor", broker)
                .put("fund", fund);
        if (features != null)
        {
            jsonUniqueKeys.put("features", new JSONArray(features));
        }
        else
        {
            jsonUniqueKeys.put("features", (Object) null);
        }
        uniqueKeys = jsonUniqueKeys.toString();
        docUuid = TardisUtils.getDocUuid(Source.GENERAL, dataCls, dataClsVersion, filePattern, env, jsonUniqueKeys);
    }
}
