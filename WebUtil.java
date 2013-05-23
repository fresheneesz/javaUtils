package com.util;

import com.ning.http.client.*;
import com.ning.http.client.extra.ThrottleRequestFilter;
import com.offers.FeatureSwitch;
import com.tracking.Event;
import com.offers.util.JsonObject;
import com.offers.util.Cept;
import com.playdom.commerce.ItemEvent;
import com.playdom.commerce.DataNotFoundException;
import com.playdom.commerce.ItemException;
import com.playdom.game.PlayerNotFoundException;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.net.*;


public class WebUtil {
    private static final XMLOutputFactory xmlFactory = XMLOutputFactory.newInstance();

    private static HttpClient httpClient;
    private static AsyncHttpClient p;
    static {
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setMaxTotalConnections(2000);
        params.setDefaultMaxConnectionsPerHost(200);
        //params.setConnectionTimeout(300); //

        MultiThreadedHttpConnectionManager m = new MultiThreadedHttpConnectionManager();
        m.setParams(params);
        httpClient = new HttpClient(m);

        AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder().setCompressionEnabled(false)
                .setAllowPoolingConnection(true)
                .addRequestFilter(new ThrottleRequestFilter(100)) // throws away connection once the queue fills up to 100 requests
                .setMaximumConnectionsPerHost(300)
                .setAllowSslConnectionPool(true).build();
        p = new AsyncHttpClient(config);
    }


    public static String urlEncode(String value, String encoding) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, encoding).replace("+","%20").replace("|","%7C");
    }

    public static boolean isUrlRelative(String url) {
        return !url.contains("//");       // if there is no protocol, the url is considered relative
    }

    // appends a base onto a url if it doesn't have http://
    public static String absolutizeUrl(String baseUrl, String url) {
        if(url == null) { return null; }
        if (isUrlRelative(url)) {
            return baseUrl+ "/" + url;
        } else {
            return url;
        }
    }

    public static String addUrlParameterWithoutDuplication(String url, String parameter, String value) throws UnsupportedEncodingException {
        Map<String, String> params = new HashMap<String,String>();

        try {
            params = getParametersFromUrlString(url);
        } catch(Throwable e) {
            logger.warn("Could not get parameters from URL string to check for duplication.",e);
            return url;
        }

        for (String key : params.keySet()) {
            if (key.equalsIgnoreCase(parameter)) {
                // parameter is already in url
                logger.info("Parameter " + parameter + " is already in url " + url + ", will not overwrite");
                return url;
            }
        }

        return addUrlParameter(url,parameter,value);
    }

    public static String addUrlParameter(String url, String parameter, String value) throws UnsupportedEncodingException {
        StringBuilder s = new StringBuilder(url);
        return addUrlParameter(s, parameter, value).toString();
    }

    public static StringBuilder addUrlParameter(StringBuilder url, String parameter, String value)  {
        String separator;
        if(url.toString().contains("?")) {
            separator = "&";
        } else {
            separator = "?";
        }

        try {
            return url.append(separator).append(parameter).append("=").append(WebUtil.urlEncode(value, "UTF-8"));
        } catch(Throwable t) {
            throw new Cept(t.getMessage(), t);
        }
    }

    public static String removeUrlParameter(String url, String parameter) {
        String[] urlArr = url.split("\\?");

        // either bad url or no parameters
        if (urlArr.length < 2) {
            return url;
        }

        String host = urlArr[0];
        String params = urlArr[1];

        String[] paramArr = params.split("&");

        StringBuilder sb = new StringBuilder(host);

        if (paramArr.length > 0) {
            sb.append("?");
        }

        int count = 0;
        for (int i=0;i<paramArr.length;i++) {
            String param = paramArr[i];
            String[] paramDataArr = param.split("=");
            String name = paramDataArr[0];

            if (!name.equalsIgnoreCase(parameter)) {
                if (count != 0) {
                    sb.append("&");
                }
                sb.append(param);
                count ++;
            }
        }

        return sb.toString();
    }

    public static Map<String, String> getParametersFromUrlString(String url) throws Throwable {
        URL urlObject = new URL(url);
        String[] arguments = urlObject.getQuery().split("&");

        Map<String, String> parameterMap = new HashMap<String, String>();
        for(String argument : arguments) {
            String[] parts = argument.split("=");
            if (parts.length > 1) {
                parameterMap.put(parts[0], URLDecoder.decode(parts[1], "UTF-8"));
            } else {
                parameterMap.put(parts[0],"");
            }
        }

        return parameterMap;
    }


    /*
    public static String getIpAddress(HttpServletRequest req, Logger logger) {
        String ipAddressForwardedStr = req.getHeader("x-forwarded-for");
        if(ipAddressForwardedStr == null) {
            if (ServerCategory.isProduction()) logger.info("No X-Forwarded-For header! "+ TrackAction.dumpRequest(req)) ;
            return req.getRemoteAddr();
        } else {
            return ipAddressForwardedStr;
        }
    }*/

    public static String getIp(HttpServletRequest req, Logger logger) {
        String defaultIp = "0.0.0.0";
        String ipAddressForwardedStr = req.getHeader("x-forwarded-for");
        if (stringExists(ipAddressForwardedStr)) {
            //String addr = ipAddressForwardedStr.split(",")[0].trim();
            for(String addr : ipAddressForwardedStr.split(",")) {
                if(ValidateIPAddress(addr))
                    return ipAddressForwardedStr;
                else
                    logger.info("Unknown x-forwarded-for address: "+addr);
            }
            // else
            return defaultIp;
        }
        // else
        if (logger != null && !ServerCategory.isLocal()) {
            Throwable t = new RuntimeException();   // exception for its stack trace
            OfferUtil.logScribeWarning(logger, ScribeReasonEnum.NO_X_FORWARDED_FOR_HEADER, "", req, null, null, t);
        }

        String ip = req.getRemoteAddr();
        if (stringExists(ip)  && ValidateIPAddress(ipAddressForwardedStr))
            return ip;

        if (logger != null)
            OfferUtil.logScribeWarning(logger, ScribeReasonEnum.NO_VALID_X_FORWARDED_FOR_HEADER, "", req, null, null, new RuntimeException());

        return defaultIp; // todo: dummy. is it needed?
    }
    private static boolean stringExists(String addr) {
        return addr != null && addr.length() > 0;
    }

    private static boolean ValidateIPAddress( String  ipAddress )
    {
        try {
            InetAddress.getByName(ipAddress);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public static String getIpAddress(HttpServletRequest req, Logger logger) {
        return getIp(req, logger);
    }

    public static String getIpAddress(HttpServletRequest req) {
        return getIp(req, null);
    }



    public static String postRequest(String url, Map<String, String> headers,
                                         int responseLength, int timeoutInMillis) throws IOException {
        PostMethod method = new PostMethod(url);
        return makeRequest(method, headers, responseLength, timeoutInMillis);
    }
    public static String makeRequest(HttpMethodBase method, Map<String, String> headers,
                                         int responseLength, int timeoutInMillis) throws IOException {
        return makeRequest(method, httpClient, headers,  responseLength, timeoutInMillis);
    }
    @Deprecated
    public static String makeRequest(final HttpMethodBase method, final HttpClient client, Map<String, String> headers,
                                         final int responseLength, int timeoutInMillis) throws IOException {
        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));
        method.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        //method.getParams().setSoTimeout(timeoutInMillis);

        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                method.setRequestHeader(entry.getKey(), entry.getValue());
            }
        }

        try {

            // Read the response body.
            String responseBody = OfferUtil.Timeout.execute(timeoutInMillis, new OfferUtil.Goable<String, Object>() {
                public String go(Object ignored) throws IOException {
                    // Execute the method.
                    int statusCode = client.executeMethod(method);

                    // does HttpMethodBase properly handle responses like redirect responses 302 303 and 307, etc? or do we have to manaully handle it?
                    if (statusCode != HttpStatus.SC_OK) {
                        throw new Cept("failedHttpRequest", "Method failed for "+method.getURI()+": " + method.getStatusLine());
                    }

                    if (responseLength > 0) {
                        return method.getResponseBodyAsString(responseLength);
                    }
                    else {
                        return method.getResponseBodyAsString();
                    }
                }
            });

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            // (^ probably unncesssary using getResponseBodyAsString, as it goes through EncodingUtil) --vf
            return responseBody;

        } catch(OfferUtil.Timeout.Exception e) {
            method.abort();
            throw e;
        } catch(IOException e) {
            throw e;
        } catch(RuntimeException e) {
            throw e;
        } catch(Throwable e) {
            throw OfferUtil.wrapException("", e);
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
    }

    // makes an http request but discards the response and returns asynchronously
    public static void fireAndForget(String url, String httpMethod) {

        if(FeatureSwitch.isFeatureOn(FeatureSwitch.Names.UISWarmupUsingAsync,null,null)) {
            try {
                AsyncHttpClient.BoundRequestBuilder requestBuilder;
                if(httpMethod == "GET") requestBuilder = p.prepareGet(url);
                else if(httpMethod == "PUT") requestBuilder = p.preparePut(url);
                else if(httpMethod == "POST") requestBuilder = p.preparePost(url);
                else if(httpMethod == "DELETE") requestBuilder = p.prepareDelete(url);
                else throw new Cept("Unsupported method: "+httpMethod);

                requestBuilder.execute(new AsyncHandler() {

                    public STATE onStatusReceived(HttpResponseStatus status) throws Exception {
                        return STATE.ABORT;
                    }
                    public STATE onHeadersReceived(HttpResponseHeaders h) throws Exception {
                        return STATE.ABORT;
                    }
                    public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                        return STATE.ABORT;
                    }
                    public String onCompleted() throws Exception {
                        return null;
                    }
                    public void onThrowable(Throwable t) {
                        logger.info("Problem",t);
                    }
                });
            } catch(Throwable t) {
                logger.info("fireAndForget exception: ",t);
            }
        } else {  // todo: remove this codebranch after testing to see if method.releaseConnection() makes a difference with the runaway thread issue we saw
            final HttpMethodBase method;
            if(httpMethod == "GET") method = new GetMethod(url);
            else if(httpMethod == "PUT") method = new PutMethod(url);
            else if(httpMethod == "POST") method = new PostMethod(url);
            else if(httpMethod == "DELETE") method = new DeleteMethod(url);
            else throw new Cept("Unsupported method: "+httpMethod);

            OfferUtil.GeneralRunnable<Object, Object> r = new OfferUtil.GeneralRunnable<Object, Object>(new OfferUtil.Goable<Object, Object>() {
                public Object go(Object ignored) throws IOException {
                    try {
                        // Execute the method.
                        httpClient.executeMethod(method);
                        method.abort();
                    } catch(Throwable t) {
                        logger.info("fireAndForget exception: ",t);
                    } finally {
                        method.releaseConnection();
                    }

                    return null;
                }
            });

            Thread serviceThread = new Thread(r);
            serviceThread.start();
        }
    }

    public static int defaultResponseLength = 4096;
    public static int largeResponseLength = 150000;
    public static int defaultTimeout = 8000;
    public static String getPageAsString(String url) throws IOException {
         return getPageAsString(url, defaultTimeout);
    }
    public static String getPageAsString(String url, int timeoutInMills) throws IOException {
        return getPageAsString(url, null, defaultResponseLength, timeoutInMills);
    }
    public static String getPageAsString(String url, Map<String, String> headers, int responseLength, int timeoutInMillis) throws IOException {
        // Create a method instance.
        GetMethod method = new GetMethod(url);
        return makeRequest(method, headers, responseLength, timeoutInMillis);
    }

    public static String getLargePageAsString(String url) throws IOException {
        return getPageAsString(url, null, largeResponseLength, 10000);
    }
    public static String getLargePageAsString(String url, int timeoutInMills) throws IOException {
        return getPageAsString(url, null, largeResponseLength, timeoutInMills);
    }
    public static String getLargePageAsStringWithCooldown(String cooldownKey, String url, int timeoutInMills) throws Throwable {
        return getPageAsStringWithCooldown(cooldownKey, url, timeoutInMills, largeResponseLength);
    }

    // returns the default result ONLY if cooling down
    public static String getPageAsStringWithCooldown(String cooldownKey, String url, int timeoutInMills, String defaultResult) throws Throwable {
        try {
            return getPageAsStringWithCooldown(cooldownKey, url, timeoutInMills);
        } catch(Throwable t) {
            if("coolingDown".equals(t.getMessage())) {
                return defaultResult;
            } else {
                throw t;
            }
        }
    }

    // sets up cooldown counter for timeouts (which are mainly expected to be timeouts)
    // throws an exception when in cooldown mode and when an exceptions happens
    private static OfferUtil.KeyedErrorCooldownCounter<String> getPageAsStringCooldownCounter = new OfferUtil.KeyedErrorCooldownCounter<String>(2, 40, .5f);
    public static String getPageAsStringWithCooldown(String cooldownKey, String url, int timeoutInMills, int responseLength) throws Throwable {
        if(getPageAsStringCooldownCounter.attempt(cooldownKey)) {
            try {
                return getPageAsString(url, null, responseLength, timeoutInMills);
            } catch(Throwable t) {
                getPageAsStringCooldownCounter.failure(cooldownKey);
                throw t;
            }
        } else {
            RuntimeException r = new Cept("coolingDown", "coolingDown");
            r.initCause(new RuntimeException("Cooling Down "+cooldownKey+" for url: "+url));
            throw r;
        }
    }
    public static String getPageAsStringWithCooldown(String cooldownKey, String url, int timeoutInMills) throws Throwable {
        return getPageAsStringWithCooldown(cooldownKey, url, timeoutInMills, defaultResponseLength);
    }
    public static String getPageAsStringWithCooldown(String cooldownKey, String url) throws Throwable {
        return getPageAsStringWithCooldown(cooldownKey, url, defaultTimeout, defaultResponseLength);
    }


    @Deprecated // shouldn't be "silently" failing on exceptions (should throw them), and shouldn't use passed in http client
    // TODO: Check every exception and use log4j exception reporting
    // Post a request to specified URL. Get response as a string.
    public static String postXMLRequestToUrl(HttpClient client, String url, String request) {
        // In our case the request usually consists of some xml that asks a third part to do some transaction

        PostMethod post = new PostMethod(url);

        // Provide custom retry handler is necessary
        post.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        // Use a 10-second timeout
        post.getParams().setIntParameter(HttpMethodParams.SO_TIMEOUT, 20000);

        try {
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "utf-8");
            post.setRequestEntity(entity);
            long time = System.currentTimeMillis();
            int statusCode = client.executeMethod(post);
            Event.logTimingEvent(url, System.currentTimeMillis() - time);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + post.getStatusLine());
                logger.info("Post failed to: " + url + ". With status: " + post.getStatusLine());
            }

            // Read the response body.
            String responseBody = post.getResponseBodyAsString(8192);
            //logger.info("POST Headers: "+headersAsString(post.getResponseHeaders()));

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            // (^ probably unncesssary using getResponseBodyAsString, as it goes through EncodingUtil) --vf
            return responseBody;
        } catch (HttpException e) {
            logger.warn("Fatal protocol violation: " + e.getMessage(),e);
        } catch (IOException e) {
            logger.info("Fatal transport error: " + e.getMessage(),e);
        } finally {
            // Release the connection.
            post.releaseConnection();
        }

        return null;
    }

    public static int postXMLRequestToUrlGetStatusCode(String url, String request) {
        // In our case the request usually consists of some xml that asks a third part to do some transaction

        PostMethod post = new PostMethod(url);

        // Provide custom retry handler is necessary
        post.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        // Use a 10-second timeout
        post.getParams().setIntParameter(HttpMethodParams.SO_TIMEOUT, 20000);

        try {
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "utf-8");
            post.setRequestEntity(entity);
            long time = System.currentTimeMillis();
            int statusCode = httpClient.executeMethod(post);
            Event.logTimingEvent(url, System.currentTimeMillis() - time);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + post.getStatusLine());
                logger.info("Post failed to: " + url + ". With status: " + post.getStatusLine());
            }

            return statusCode;
        } catch (HttpException e) {
            logger.warn("Fatal protocol violation: " + e.getMessage(),e);
        } catch (IOException e) {
            logger.info("Fatal transport error: " + e.getMessage(),e);
        } finally {
            // Release the connection.
            post.releaseConnection();
        }

        return HttpStatus.SC_GONE;
    }

    // returns an object like:
        // { response: <response>
        //   status: <status>
        // }
    public static JsonObject post(String url, String contentType, String requestBody, int timeout) throws ConnectTimeoutException {
        JsonObject result = JsonObject.make();

        PostMethod post = new PostMethod(url);
            post.getParams().setIntParameter(HttpMethodParams.SO_TIMEOUT, timeout);

        try {
            post.setRequestEntity(new StringRequestEntity(requestBody, contentType, "utf-8"));
            result.put("status", httpClient.executeMethod(post));
            result.put("response", post.getResponseBodyAsString(10000)); // max 10kb
            return result;

        } catch(ConnectTimeoutException t) {
            throw t;
        } catch (Throwable t) {
            RuntimeException r = new RuntimeException("Problem posting to "+url);
            r.initCause(t);
            throw r;

        } finally {
            // Release the connection.
            post.releaseConnection();
        }
    }

    public static String delete(String url, String contentType, int timeout) throws ConnectTimeoutException {
        JsonObject result = JsonObject.make();

        try {
            URL urlObj = new URL(url);
            sun.net.www.protocol.https.HttpsURLConnectionImpl httpCon = (sun.net.www.protocol.https.HttpsURLConnectionImpl)urlObj.openConnection();
            httpCon.setDoOutput(true);
            httpCon.setRequestProperty("Content-Type","application/x-www-form-urlencoded");
            httpCon.setRequestMethod("DELETE");
            httpCon.connect();

            Reader reader = new InputStreamReader(httpCon.getInputStream(),"UTF-8");

            StringBuilder output = new StringBuilder();
            char[] buffer = new char[1024];

            try {
                while(true) {
                    int read = reader.read(buffer,0,buffer.length);
                    if (read < 0) break;
                    output.append(buffer);
                }
            } finally {
                reader.close();
            }

            return output.toString();
        } catch (Throwable t) {
            RuntimeException r = new RuntimeException("Problem posting to "+url);
            r.initCause(t);
            throw r;

        }
    }

    private static String headersAsString(Header[] headers) {
        StringBuilder sb = new StringBuilder();
        for (Header h : headers) {
            sb.append(h.toString());
        }
        return sb.toString();
    }

    public static String constructXML(Object data) {
        return constructXML(data, "xml");
    }

    public static String constructXML(Object data, String topLevelTag) {
        if (topLevelTag == null || topLevelTag.isEmpty()) {
            return constructXML(data);
        }
        StringWriter resultWriter = new StringWriter();
        try {
            XMLStreamWriter xmlWriter = xmlFactory.createXMLStreamWriter(resultWriter);
            if (data == null) {
                constructXMLHelper(new Pair(topLevelTag, "null"), xmlWriter);
            } else {
                constructXMLHelper(new Pair(topLevelTag, data), xmlWriter);
            }
            return resultWriter.toString();
        } catch (XMLStreamException e) {
            return "";
        }
    }

    public static String constructXMLFragment(Object data) {
        StringWriter resultWriter = new StringWriter();
        try {
            XMLStreamWriter xmlWriter = xmlFactory.createXMLStreamWriter(resultWriter);
            if (data == null) {
                constructXMLHelper("null", xmlWriter);
            } else {
                constructXMLHelper(data, xmlWriter);
            }
            return resultWriter.toString();
        } catch (XMLStreamException e) {
            return "";
        }
    }

    protected static void constructXMLHelper(Object data, XMLStreamWriter xmlWriter) throws XMLStreamException {
        if (data == null) {
            logger.error("Data in xml should not be null");
        } else if (data instanceof Pair) {
            Pair pair = (Pair) data;
            String tag = pair.getFirst().toString();
            xmlWriter.writeStartElement(tag);

            if (pair.getSecond() == null) {
                logger.error(tag + " tag has null data");
            } else {
                constructXMLHelper(pair.getSecond(), xmlWriter);
            }
            xmlWriter.writeEndElement();
        } else if (data instanceof List) {
            List list = (List) data;
            if (!list.isEmpty() && list.get(0) instanceof Pair) {
                List<Pair> pairs = (List<Pair>) list;
                for (Pair pair : pairs) {
                    constructXMLHelper(pair, xmlWriter);
                }
            }
        } else {
            xmlWriter.writeCharacters(data.toString());
        }
    }
}

