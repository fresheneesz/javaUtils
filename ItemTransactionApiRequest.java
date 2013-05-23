package com.api;

import com.util.OfferUtil;
import com.util.WebUtil;
import com.offers.util.OffersHttpServletRequest;
import com.offers.util.JsonObject;
import com.offers.util.JsonArray;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ItemTransactionApiRequest extends AuthenticatedMonetizationApiRequest {
    private static final Logger logger = LoggerFactory.getLogger(ItemTransactionApiRequest.class);

    public static class Item extends JsonObject {
        public static List<Item> makeItems(JsonArray a) {
            List<Item> list = new ArrayList<Item>();
            for(Object obj : a) { JsonObject o = (JsonObject) obj;
                list.add(new Item(o));
            }
            return list;
        }
        private String category, id;
        int amount;
        JsonObject info;

        public Item(JsonObject o) {
            merge(o);
            initMembers();
        }
        public Item(String category, String id, int amount, JsonObject info) {
            put("category", category);
            put("id", id);
            put("amount", amount);
            put("info", info);

            initMembers();
        }

        private void initMembers() {
            category = getString("id");
            id = getString("id");
            amount = getInt("amount");
            if(has("info")) info = getJSONObject("info");
        }
    }


    private String idOrigin, id, network, user;
    List<Item> items;

    public ItemTransactionApiRequest(OffersHttpServletRequest r, String secret) throws Throwable {
        super(r,secret);
        initMembers();
    }
    public ItemTransactionApiRequest(String secret, String system, String requester, String comment, JsonObject info, Date t,
                                    String idOrigin, String id, String network, String user, JsonArray items) {
        super(secret, system, requester, comment, info, t);
        put("idOrigin", idOrigin);
        put("id", id);
        put("network", network);
        put("user", user);
        put("items", items);
        initMembers();
    }

    private void initMembers() {
        idOrigin = getString("idOrigin");
        id = getString("id");
        network = getString("network");
        user = getString("user");
        items = Item.makeItems(getJSONArray("items"));
    }
}

