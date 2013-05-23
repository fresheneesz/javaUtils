package com.offers.util; 

// An exception with a "code" as well as a message
public class Cept extends RuntimeException {
    String code;

    public Cept(String code, String msg, Throwable cause) {
        super(msg);
        if(cause!=null) initCause(cause);
        this.code = code;
    }
    public Cept(String code) {
        this(code, "", null);
    }
    public Cept(String code, Throwable cause) {
        this(code, "", cause);
    }
    public Cept(String code, String msg) {
        this(code, msg, null);
    }

    public String getCode() {
        return code;
    }
}