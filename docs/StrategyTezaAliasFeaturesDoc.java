package com.teza.common.tardis.docs;

import com.teza.common.tardis.DocImpl;
import com.teza.common.tardis.Source;
import com.teza.common.tardis.TardisUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * User: tom
 * Date: 1/23/17
 * Time: 12:44 PM
 */
public class StrategyTezaAliasFeaturesDoc extends DocImpl
{
    private static final String filePattern = "{source}/{strategy}_{version}/{teza_alias}_{from_date}_{to_date}_{content}.bin.gz";

    public static String cleanTezaAlias(String tezaAlias)
    {
        return tezaAlias.replace(":", "_").replace("#", "_");
    }

    public StrategyTezaAliasFeaturesDoc(String strategy, String version, String tezaAlias)
    {
        this(strategy, version, tezaAlias, null);
    }

    public StrategyTezaAliasFeaturesDoc(String strategy, String version, String tezaAlias, List<String> features)
    {
        super(Source.DERIVED, new JSONObject(), filePattern, null);
        if (!TardisUtils.isValidFileNamePart(strategy) ||
                !TardisUtils.isValidFileNamePart(version) ||
                !TardisUtils.isValidFileNamePart(tezaAlias))
        {
            throw new IllegalArgumentException("invalid strategy, version, or teza aliase: (" +
                    strategy + ", " + version + ", " + tezaAlias + ")");
        }

        JSONObject jsonUniqueKeys = new JSONObject()
                .put("strategy", strategy)
                .put("version", version)
                .put("teza_alias", tezaAlias);
        if (features != null)
        {
            jsonUniqueKeys.put("features", new JSONArray(features));
        }
        else
        {
            jsonUniqueKeys.put("features", (Object) null);
        }
        uniqueKeys = jsonUniqueKeys.toString();
        docUuid = TardisUtils.getDocUuid(Source.DERIVED, dataCls, dataClsVersion, filePattern, env, jsonUniqueKeys);
    }
}
