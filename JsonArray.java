package com.offers.util;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class JsonArray implements Iterable<Object> {
    List<Object> elements = new ArrayList<Object>();

    // can return null! (unlike the contructor)
    public static JsonArray make(String s) {
        if(s==null) {
            return null;
        } else {
            try {
                JsonArray result = new JsonArray();
                result.add(new JSONArray(s));
                return result;
            } catch(JSONException e) {
                JsonObject.JsonException j = new JsonObject.JsonException("Parsing problem");
                j.initCause(e);
                throw j;
            }
        }
    }
    public static JsonArray make(JSONArray a) {
        if(a==null) {
            return null;
        } else {
            JsonArray result = new JsonArray();
            result.add(a);
            return result;
        }
    }
    public static JsonArray make(List a) {
        if(a==null) {
            return null;
        } else {
            JsonArray ja = new JsonArray();
            ja.add(a);
            return ja;
        }
    }


    public JsonArray() {}

    public JSONArray toJSONArray() {
        JSONArray a = new JSONArray();
        for(Object v : elements) {
            if(v instanceof JsonObject) a.put(((JsonObject)v).toJSONObject());
            else if(v instanceof JsonArray) a.put(((JsonArray)v).toJSONArray());
            else a.put(v);
        }
        return a;
    }

    public JsonArray put(Object x) {
        elements.add(x);
        return this;
    }

    public JsonArray getJSONArray(int key) {
        return get(key, JsonArray.class);
    }

    private <T> T get(int key, Class<T> type) {
        Object value = elements.get(key);
        if(type.isInstance(value)) throw new JsonObject.JsonException(key +" does not contain a "+type+" value.");
        return (T) value;
    }

    public JsonArray add(JSONArray newElements) {
        for(int n=0; n<newElements.length(); n++) {
            Object value;
            try {
                value = newElements.get(n);
            } catch(JSONException e) {
                JsonObject.JsonException j = new JsonObject.JsonException("Internal exception");
                j.initCause(e);
                throw j;
            }

            if(value instanceof JSONObject) {
                elements.add(JsonObject.make((JSONObject)value));
            } else if(value instanceof JSONArray) {
                elements.add(JsonArray.make((JSONArray)value));
            } else {
                elements.add(value);
            }
        }
        return this;
    }
    public void add(List newMembers) {
        for(Object x : newMembers) {
            elements.add(x);
        }
    }

    public boolean contains(Object o) {
        return elements.contains(o);
    }

    public List toList() {
        return elements;
    }

    public Iterator<Object> iterator() {
        return elements.iterator();
    }

    public String toString() {
        return toJSONArray().toString();
    }
}
