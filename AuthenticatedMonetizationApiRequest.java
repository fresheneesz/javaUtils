package com.api;

import com.offers.util.JsonObject;
import com.util.SecurityUtil;
import com.util.OfferUtil;
import com.offers.util.OffersHttpServletRequest;

import java.util.Date;


public class AuthenticatedMonetizationApiRequest extends MonetizationApiRequest {

    private String secret;

    public class Unauthorized extends RuntimeException {
        public Unauthorized(String s) {super(s);}
    }

    public AuthenticatedMonetizationApiRequest(OffersHttpServletRequest r, String secret) throws Throwable {
        parseRequest(getRequest(r));
        initMembers(secret);
    }
    public AuthenticatedMonetizationApiRequest(String secret, String system, String requester, String comment, JsonObject info, Date t) {
        super(system, requester, comment, info, t);
        initMembers(secret);
    }

    @Override
    protected String getRequest(OffersHttpServletRequest r) throws Throwable {
        String rawBody = super.getRequest(r);
        int dividingIndex = rawBody.indexOf(" ");
        String hash = rawBody.substring(0, dividingIndex);
        String jsonRequest = rawBody.substring(dividingIndex+1);

        // validate hash
        String calculatedHash = calculateHash(jsonRequest);
        if( ! calculatedHash.equals(hash) ) {
            throw new Unauthorized("Hash mismatch between given hash: "+hash+" and calculated hash: "+calculatedHash);
        }

        return jsonRequest;
    }

    private void initMembers(String secret) {
        this.secret = secret;
    }

    private String calculateHash(String rawRequest) throws Throwable {
        return SecurityUtil.sha1HMAC(secret,rawRequest);
    }

    @Override public String toString() {
        String rawRequest = super.toString();
        try {
            return calculateHash(rawRequest)+" "+rawRequest;
        } catch(Throwable t) {
            throw OfferUtil.wrapException("Exception stringifying AuthenticatedMonetizationApiRequest", t);
        }
    }

}

