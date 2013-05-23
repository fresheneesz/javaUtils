package com.offers.util;

import org.json.JSONObject;
import org.json.JSONException;
import org.json.JSONArray;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class JsonObject {
    Map<String,Object> members = new HashMap<String,Object>();

    public static class JsonException extends RuntimeException { public JsonException(String s) {super(s);}} // thanks for helping me out, java inheritance...

    public static JsonObject make() {
        return new JsonObject();   
    }

    // can return null! (unlike the contructor)
    public static JsonObject make(String s) {
        if(s==null) return null;
        else {
            try {
                return make(new JSONObject(s));
            } catch(JSONException e) {
                JsonException j = new JsonException("Parsing problem");
                j.initCause(e);
                throw j;
            }
        }
    }
    public static JsonObject make(JSONObject o) {
        if(o==null) {
            return null;
        } else {
            JsonObject jo = new JsonObject();
            jo.merge(o);
            return jo;
        }
    }

    // returns if stuff is parsable as JSON
    // returns false for null
    public static boolean isJson(String stuff) {
        if(stuff == null) return false;
        
        try {
            JsonObject.make(stuff);
            return true;
        } catch(Throwable problem) {
            return false;
        }
    }

    /*public static JsonObject copy(JSONObject o) throws JSONException {
        if(o==null) {
            return null;
        } else {
            JsonObject jo = new JsonObject();
            jo.merge(o);
            return jo;
        }
    } */

    public JsonObject() {}

    public JSONObject toJSONObject() {
        try {
            JSONObject o = new JSONObject();
            for(String k : members.keySet()) { Object v = members.get(k);
                if(v instanceof JsonObject) o.put(k, ((JsonObject)v).toJSONObject());
                else if(v instanceof JsonArray) o.put(k, ((JsonArray)v).toJSONArray());
                else if(v == null) o.put(k, JSONObject.NULL);
                else o.put(k, v);
            }
            return o;
        } catch(JSONException e) {
            JsonException j = new JsonException("Problem converting to a JSONObject");
            j.initCause(e);
            throw j;
        }
    }

    public JsonObject copy() {
        JsonObject newObj = new JsonObject();
        newObj.merge(this);
        return newObj;
    }

    public boolean has(String key) {
        return keys().contains(key);
    }
    public boolean isNull(String key) {   // todo: get rid of this once accessors don't return null when they don't have the member
        return keys().contains(key) && members.get(key) == null;
    }
    public Set<String> keys() {
        return members.keySet();
    }

    public void remove(String key) {
        members.remove(key);
    }

    public JsonObject put(String key, Object value) {
        members.put(key, value);
        return this;
    }


    private <T> T get(String key, Class<T> type) {
        Object value = getValue(key);
        if(value == null) return null;

        return returnValue(key,value,type);
    }
    private <T> T returnValue(String key, Object value, Class<T> type) {
        validateType(key,value,type);
        return (T) value;
    }
    private Object getValue(String key) {
        if(isNull(key) || !has(key) ) return null;
        return members.get(key);
    }
    private <T> void validateType(String key, Object value, Class<T> type) {
        if( ! type.isInstance(value)) throw new JsonException(key +" does not contain a "+type+" value.");
    }

    public Boolean getBool(String key) {
        return get(key, Boolean.class);
    }
    public String getString(String key) {
        Object value = getValue(key);
        if(value == null) return null;

        if(value instanceof Long) return value.toString();
        else if(value instanceof Integer) return value.toString();
        else return returnValue(key,value,String.class);
    }
    public Integer getInt(String key) {
        Object value = getValue(key);
        if(value == null) return null;

        if(value instanceof Long) return ((Long)value).intValue();
        else if(value instanceof String) return Integer.parseInt((String)value);
        else return returnValue(key,value,Integer.class);
    }
    public Long getLong(String key) {
        Object value = getValue(key);
        if(value == null) return null;

        if(value instanceof Integer) return ((Integer)value).longValue();
        else if(value instanceof String) return Long.parseLong((String)value);
        else return returnValue(key,value,Long.class);
    }
    public JsonObject getJSONObject(String key) {
        return get(key, JsonObject.class);
    }
    public JsonArray getJSONArray(String key) {
        return get(key, JsonArray.class);
    }

    public JsonObject merge(JSONObject newMembers) {
        if(newMembers != null) {            
            Iterator<String> i = newMembers.keys();
            while(i.hasNext()) { String key = i.next();
                try {
                    Object value = newMembers.get(key);
                    if(value == JSONObject.NULL) {
                       members.put(key, null);
                    } else if(value instanceof JSONObject) {
                        members.put(key, JsonObject.make((JSONObject)value));
                    } else if(value instanceof JSONArray) {
                        members.put(key, JsonArray.make((JSONArray)value));
                    } else {
                        members.put(key, value);
                    }
                } catch(JSONException e) {
                    JsonException j = new JsonException("Internal exception");
                    j.initCause(e);
                    throw j;
                }
            }
        }
        return this;
    }

    public JsonObject merge(JsonObject newMembers) {
        for(String k : newMembers.members.keySet()) {
            members.put(k, newMembers.members.get(k));
        }
        return this;
    }

    public String toString() {
        return toJSONObject().toString();   
    }

}
