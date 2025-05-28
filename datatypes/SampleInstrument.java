package com.teza.common.tardis.datatypes;

import com.teza.common.util.TradingVenue;

import java.util.Set;
import java.util.TreeSet;

/**
 * User: tom
 * Date: 7/6/17
 * Time: 12:59 PM
 */
public class SampleInstrument
{
    private final Set<String> tezaAliases = new TreeSet<String>();
    private final String tezaKey;
    private final String tezaProduct;
    private final String primaryTradingVenue;
    private final String domain;
    private final int tezaId;
    private final double tickSize;

    public SampleInstrument(String tezaKey, String tezaProduct, int tezaId, String primaryTradingVenue, String domain)
    {
        this(tezaKey, tezaProduct, tezaId, -1, primaryTradingVenue, domain);
    }

    public SampleInstrument(String tezaKey, String tezaProduct, int tezaId, double tickSize, String primaryTradingVenue, String domain)
    {
        this.tezaKey = tezaKey;
        this.tezaProduct = tezaProduct;
        this.tezaId = tezaId;
        this.tickSize = tickSize;
        this.primaryTradingVenue = primaryTradingVenue;
        this.domain = domain;
    }

    public String getDomain()
    {
        return domain;
    }

    public String getTezaKey()
    {
        return tezaKey;
    }

    public String getTezaProduct() {
        return tezaProduct;
    }

    public int getTezaId()
    {
        return tezaId;
    }

    public double getTickSize()
    {
        return tickSize;
    }

    public TradingVenue getPrimaryTradingVenue() {
        return primaryTradingVenue == null ? null : TradingVenue.valueOf(primaryTradingVenue);
    }

    public Set<String> getTezaAliases()
    {
        return tezaAliases;
    }

    public SampleInstrument addTezaAlias(String tezaAlias)
    {
        if (tezaAlias != null && !tezaAlias.isEmpty())
        {
            tezaAliases.add(tezaAlias);
        }
        return this;
    }

    public SampleInstrument addTezaAlias(SampleInstrument i)
    {
        if (i != null)
        {
            i.tezaAliases.forEach(this::addTezaAlias);
        }
        return this;
    }

    public int removeTezaAlias(String tezaAlias)
    {
        tezaAliases.remove(tezaAlias);
        return tezaAliases.size();
    }

    public int removeTezaAlias(SampleInstrument i)
    {
        tezaAliases.removeAll(i.tezaAliases);
        return tezaAliases.size();
    }

    public SampleInstrument copy()
    {
        SampleInstrument i = new SampleInstrument(tezaKey, tezaProduct, tezaId, tickSize, primaryTradingVenue, domain);
        return i.addTezaAlias(this);
    }

    public String serialize()
    {
        return tezaKey + "|" + tezaProduct + "|" + tezaId + "|" + (Double.isNaN(tickSize) ? "" : tickSize) + "|" + (primaryTradingVenue == null ? "" : TradingVenue.valueOf(primaryTradingVenue).toString()) + "|" + domain;
    }

    public static SampleInstrument deserialize(String serialized)
    {
        final int tezaKey = 0, tezaProduct = 1, tezaId = 2, tickSize = 3, primaryTradingVenue = 4, domain = 5;
        String[] parts = serialized.split("\\|", -1);
        String venue = parts[primaryTradingVenue].equals("") ? null : parts[primaryTradingVenue];
        double ts = parts[tickSize].equals("") ? Double.NaN : Double.parseDouble(parts[tickSize]);
        return new SampleInstrument(parts[tezaKey], parts[tezaProduct], Integer.parseInt(parts[tezaId]), ts, venue, parts[domain]);
    }
}
