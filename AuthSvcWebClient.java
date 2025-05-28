package com.teza.common.tardis;

import com.sun.jersey.api.client.WebResource;
import com.teza.common.datasvcs.client.ClientContextImpl;
import com.teza.common.datasvcs.jax.AbstractWebClient;
import com.teza.common.util.AuthdHttpSession;
import org.json.JSONObject;

import javax.ws.rs.core.Cookie;
import java.net.URISyntaxException;

/**
 * User: tom
 * Date: 1/31/17
 * Time: 2:43 PM
 */
public class AuthSvcWebClient extends AbstractWebClient
{
    public AuthSvcWebClient(String host, int port)
    {
        super(host, port, false, "/authsvc", new ClientContextImpl(null));
    }

    public Cookie getSessionKey()
    {
        return getSessionKey(86400);
    }

    public Cookie getSessionKey(long ttl)
    {
        long effectiveTtl = ttl > 86400 ? 86400 : ttl;
        WebResource resource;
        try
        {
            resource = getJaxClient().resource(getUrlService().resolveUrl("/rest/auth/gt/" + effectiveTtl).toURI());
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
        JSONObject resp = resource.cookie(null).get(JSONObject.class);
        if (resp.has("token"))
        {
            return new Cookie(AuthdHttpSession.PUBLIC_SESSION_KEY_COOKIE, resp.getString("token"));
        }
        throw new RuntimeException("could not find token from " + resp);
    }
}
