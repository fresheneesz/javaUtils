package com.offers.util;

import com.util.OfferUtil;
import com.util.ScribeReasonEnum;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.ServletInputStream;
import java.util.*;
import java.io.*;
import java.net.URLDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Billy Tetrud
 * Date: Nov 30, 2011
 * Some of this code stole from  http://www.coderanch.com/t/361348/Servlets/java/Servlet-Filter-Reading-Request-Error
 * Main purpose of this is to override a few methods that return bad information for our app (stuff related to the protocol and post data)
 */
public class OffersHttpServletRequest extends HttpServletRequestWrapper {
    private static final int INITIAL_BUFFER_SIZE = 1024;
    private byte[] reqBytes=null;
    private Map<String, String[]> parameterMap = null;

    private static final Logger logger = LoggerFactory.getLogger(OffersHttpServletRequest.class);

    public OffersHttpServletRequest(HttpServletRequest r) {
        super(r);
    }

    public String getRequestBody() throws IOException {
        return new String(getRawRequestBody());
    }

    public String getRequestBody(String encoding) throws IOException {
        return new String(getRawRequestBody(), encoding);
    }

    public byte[] getRawRequestBody() throws IOException {
        popuplateReqBytes();
        return reqBytes;
    }
    public String getParameter(String name) {
        try {
            popuplateReqBytes();
        }
        catch(IOException e) {
            RuntimeException r = new RuntimeException();
            r.initCause(e);
            throw r;
        }

        String[] a = getParameterMap().get(name);
        if (a == null || a.length == 0) {
            return null;
        }
        return a[0];
    }

    public Enumeration getParameterNames() {
        return Collections.enumeration(parameterMap.keySet());
    }

    public String[] getParameterValues(String name) {
        return parameterMap.get(name);
    }

    public String getFullURL() {
        StringBuilder fullUrl = new StringBuilder(getRequestURL().toString());

        String queryString = getQueryString();
        if (queryString == null) {
            return fullUrl.toString();
        } else {
            return fullUrl.append('?').append(queryString).toString();
        }
    }

    @Override public String getProtocol() {
        String serverHostName = getHeader("host");
        String protocolName = getHeader("x-forwarded-proto");

        if(serverHostName.equals("offer-qa01-external.vm.dfw.playdom.com:8080")) {
            return "http";

        } else if(protocolName != null) {
            return protocolName;

        } else {
            String requestURL = super.getRequestURL().toString();
            if("http:".equals(requestURL.substring(0,5))) {
                return "http";
            } else {
                return "https";
            }
        }
    }

    @Override public StringBuffer getRequestURL() {
        String resultUrl = super.getRequestURL().toString();

        int n = resultUrl.indexOf(':');
        resultUrl = resultUrl.replaceFirst(resultUrl.substring(0,n), getProtocol());

        return new StringBuffer(resultUrl);
    }

    @Override public boolean isSecure() {
        return "https".equals(getProtocol());
    }

    @Override public BufferedReader getReader() throws IOException {
        popuplateReqBytes();
        InputStreamReader dave = new InputStreamReader(new ByteArrayInputStream(reqBytes));
        return new BufferedReader(dave);
    }

    @Override public ServletInputStream getInputStream() throws IOException {
        popuplateReqBytes();

        return new ServletInputStream() {
            private int numberOfBytesAlreadyRead;

            @Override
            public int read() throws IOException {
                byte b;
                if (reqBytes.length > numberOfBytesAlreadyRead) {
                    b = reqBytes[numberOfBytesAlreadyRead++];
                } else {
                    b = -1;
                }

                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (len > (reqBytes.length - numberOfBytesAlreadyRead)) {
                    len = reqBytes.length - numberOfBytesAlreadyRead;
                }
                if (len <= 0) {
                    return -1;
                }
                System.arraycopy(reqBytes, numberOfBytesAlreadyRead, b, off, len);
                numberOfBytesAlreadyRead += len;
                return len;
            }

        };
    }

    @Override public Map<String, String[]> getParameterMap() {
        if (parameterMap == null) {
            parameterMap = new HashMap<String, String[]>();
            try {
                addAllToMap(parameterMap, parseQuery(getQueryString()));
                addAllToMap(parameterMap, parseQuery(getRequestBody()));
            } catch(UnsupportedEncodingException x) {
                logger.info("getParameterMap exception: ", x);
            } catch(IOException x) {
                logger.info("getParameterMap exception: ", x);
            }
        }
        return parameterMap;
    }

    // attempts to ensure that the reqBytes is populated
    // reqBytes will get null if there is a problem reading the request
    private void popuplateReqBytes() throws IOException {
        try {
           if(reqBytes == null) {
                // Read the parameters first, because they can get unreachable after the inputStream is read.

                int initialSize = getContentLength();
                if (initialSize < INITIAL_BUFFER_SIZE) {
                    initialSize = INITIAL_BUFFER_SIZE;
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream(initialSize);
                byte[] buf = new byte[1024];
                InputStream is = super.getInputStream();
                int len = 0;
                while (len >= 0) {
                    len = is.read(buf);
                    if (len > 0) {
                        baos.write(buf, 0, len);
                    }
                }
                reqBytes = baos.toByteArray();
            }
        } catch(IOException t) {
            OfferUtil.logScribeWarning(logger, ScribeReasonEnum.IOEXCEPTION_WHEN_GETTING_POST_DATA, "Request: "+this, null, null, null, t); // don't log request normally since there is already a problem with the request
            reqBytes = new byte[]{}; // set reqBytes to something so that exceptions don't loop around
            throw t;
        }
    }

    private void addAllToMap(Map<String, String[]> mapToStringArray, Map<String, List<String>> mapToStringList) {
        for(String k : mapToStringList.keySet()) {
            mapToStringArray.put(k, (String[]) mapToStringList.get(k).toArray(new String[mapToStringList.get(k).size()]));
        }
    }


    private static Map<String, List<String>> parseQuery(String query) throws UnsupportedEncodingException {
        Map<String, List<String>> params = new HashMap<String, List<String>>();
        if(query == null || "".equals(query)) return params;
        try {
            for (String param : query.split("&")) {
                String pair[] = param.split("=");
                String key = URLDecoder.decode(pair[0], "UTF-8");
                String value = "";
                if (pair.length > 1) {
                    value = URLDecoder.decode(pair[1], "UTF-8");
                }
                List<String> values = params.get(key);
                if (values == null) {
                    values = new ArrayList<String>();
                    params.put(key, values);
                }
                values.add(value);
            }
        } catch(Throwable t) {
            logger.info(t.getMessage()+"\nPotential Problem parsing query: "+query); // but really just ignore it most of the time
        }

        return params;
    }
}
