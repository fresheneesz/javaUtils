package com.api;

import com.offers.util.BadRequest;
import com.util.WebUtil;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.json.JSONObject;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.offers.util.OffersHttpServletRequest;
import com.offers.util.JsonObject;
import com.util.OfferUtil;

import java.util.Date;
import java.util.Iterator;


public class MonetizationApiRequest extends JsonObject {
    private static final Logger logger = LoggerFactory.getLogger(MonetizationApiRequest.class);

    public String system, requester, comment=null, t;
    public JsonObject info=null;

    public MonetizationApiRequest() {}
    public MonetizationApiRequest(OffersHttpServletRequest r) throws Throwable {
        try {
            parseRequest(this.getRequest(r));
        } catch(Throwable t) {
            OfferUtil.info(logger, "Couldn't parse request: ", r, null, null, t);
            throw t;
        }
    }
    public MonetizationApiRequest(String system, String requester, String comment, JsonObject info, Date t) {
        super();

        put("system", system);
        put("requester", requester);
        if (comment != null) put("comment", comment);
        if (info != null) put("info", info);
        put("t", t.getTime());

        initMembers();
    }


    private void initMembers() {
        system = getString("system");
        requester = getString("requester");
        if(has("comment")) comment = getString("comment");
        if(has("info")) info = getJSONObject("info");
        t = getString("t");
    }

    private enum SuccessEnum {success}
    public String success() throws JSONException {
        return success(JsonObject.make());
    }
    public String success(JsonObject otherInfo) {
        String success = SuccessEnum.success.name();
        return response(responseObject(success, otherInfo));
    }

    public enum FailureEnum {temporaryFailure, permenantFailure}

    public String failure(FailureEnum result, String message)  throws JSONException {
        return failure(result, message, null, null, null);
    }
    public String failure(FailureEnum result, String message, String type, JSONObject info)  throws JSONException {
        return failure(result, message, type, info, null);
    }

    public String failure(FailureEnum result, String message, String type, JSONObject info, JSONObject otherMembers) {
        JsonObject o = JsonObject.make();
            o.put("message", message);
            if(type!=null) o.put("type", type);
            if(info!=null) o.put("info", info);
            if(otherMembers!=null) o.merge(otherMembers);

        return response(responseObject(result.name(), o));
    }

    private static OfferUtil.KeyedErrorCooldownCounter<String> monetizationApiErrorRate = new OfferUtil.KeyedErrorCooldownCounter<String>(2, 30, .5f);
    public JsonObject execute(String url, int timeout) {
        JsonObject response = null;
        try {
            if(monetizationApiErrorRate.attempt(url)) {
                logger.info("Executing request: \n"+toString());

                response = WebUtil.post(url, "text/json", toString(), timeout);
                logger.info("Got response: "+response.getString("response").toString());

                if(response.getInt("status") != 200) {
                    JsonObject result = JsonObject.make();
                    result.put("result", "temporaryFailure");
                    result.put("message", "Status "+response.getInt("status")+" returned.");
                    return result;
                } else {
                    return JsonObject.make(response.getString("response"));
                }
            } else {
                return temporaryFailure("coolingDown", null);
            }
        } catch(ConnectTimeoutException e) {
            return temporaryFailure("timeout", "Timeout: "+e.getMessage());

        } catch(JsonException e) {
            return temporaryFailure("jsonException", "Problem parsing response: "+e.getMessage());

        } catch(Throwable e) {
            throw OfferUtil.wrapException("Exception. Response: "+response, e);
        }
    }

    // protected

    protected String response(JsonObject responseObject) {
        return responseObject.toString();
    }

    protected String getRequest(OffersHttpServletRequest r) throws Throwable {
        return r.getRequestBody("UTF-8");
    }

    protected void parseRequest(String body) {
        JsonObject object = JsonObject.make(body);
        this.merge(object);
        initMembers();
    }

    // privates

    private static JsonObject responseObject(String result, JsonObject otherInfo) {
        JsonObject o = JsonObject.make();
            o.put("result", result);

        return o.merge(otherInfo);
    }

    private static JsonObject temporaryFailure(String type, String message) {
        JsonObject result = JsonObject.make();
        result.put("result", "temporaryFailure");
        result.put("type", type);
        if(message!=null) result.put("message", message);
        return result;
    }

}

