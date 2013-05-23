package com.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.codehaus.janino.ExpressionEvaluator;
import org.json.JSONException;
import org.json.JSONArray;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.text.SimpleDateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.io.*;

import com.offers.analytics.BI.BIAppMapCache;
import com.offers.util.IframeId;
import com.offers.util.JsonArray;
import com.offers.LogThreshold;
import com.offers.transaction.Transaction;
import com.offers.user.UserIdMapper;
import com.offers.user.FBThirdPartyIdMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class OfferUtil {
    

    public static List JSONArrayToList(JSONArray a) throws JSONException {
        return JsonArray.make(a).toList();
    }

    public static String langFromLocale(String locale) {
        if(locale != null) {
            return locale.split("_")[0];
        } else {
            return null;
        }
    }

    public static String implode(String separator, List data) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object s : data) {
            if(first) first = false;
            else sb.append(separator);
            sb.append(s.toString());
        }
        return sb.toString();
    }
    public static Set<String> explodeSet(String separator, String list) {
        String[] array = StringUtils.splitPreserveAllTokens(list, separator);
        return new HashSet<String>(Arrays.asList(array));
    }

    // substitutes something like {name} with a string
    public static String substitute(String text, String name, String value) {
        if(text==null) return null;
        return text.replace("{"+name+"}", value);
    }

    // I can't believe java doesn't have a built in way to do this...
    public static int longToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                (l + " cannot be cast to int without changing its value.");
        }
        return (int) l;
    }



    public static String formatNumber(Long number) {
        DecimalFormat format = new DecimalFormat("###,###,###");
        return format.format(number);
    }

    public static String formatPrice(BigDecimal number) {
        DecimalFormat format;
        if(number.remainder(new BigDecimal(1)).compareTo(new BigDecimal(0)) > 0) {
            format = new DecimalFormat("###,###,##0.00");
        } else {
            format = new DecimalFormat("###,###,##0.##");
        }

        return format.format(number);
    }

    public static String formatDateTime_ISO8601 (Date date) {
        if (date == null) {
            return formatDateTime_ISO8601(new Date());
        }

        // format in (almost) ISO8601 format
        String dateStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format (date);

        // remap the timezone from 0000 to 00:00 (starts at char 22)
        return dateStr.substring (0, 22) + ":" + dateStr.substring (22);
    }
    public static Date ISO8601_toDate(String dateString) throws java.text.ParseException {

        //NOTE: SimpleDateFormat uses GMT[-+]hh:mm for the TZ which breaks
        //things a bit.  Before we go on we have to repair this.
        SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ssz" );

        //this is zero time so we need to add that TZ indicator for
        if ( dateString.endsWith( "Z" ) ) {
            dateString = dateString.substring( 0, dateString.length() - 1) + "GMT-00:00";
        } else {
            int inset = 6;

            String s0 = dateString.substring( 0, dateString.length() - inset );
            String s1 = dateString.substring( dateString.length() - inset, dateString.length() );

            dateString = s0 + "GMT" + s1;
        }

        return df.parse(dateString);

    }

    // Change a date in another timezone
    public static String formatDateWithTimezone(Date date, String zone) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
        sdf.setTimeZone(TimeZone.getTimeZone(zone));
        return sdf.format(date);
    }

    /**
     * Converts a date into a string, formatted to be compatible with our datastore.
     * Resulting date does include HH:mm:ss.
     *
     * @param date
     * @return
     */
    public static String formatNormalDate(Date date) {
        return formatNormalDate(date, true);
    }

    /**
     * Converts a date into a string, formatted to be compatible with our datastore.
     *
     * @param date
     * @param includeTime
     * @return
     */
    public static String formatNormalDate(Date date, boolean includeTime) {
        if (date==null) return null;
        if (includeTime) {
            return formatDate(date, "yyyy-MM-dd HH:mm:ss");
        }
        return formatDate(date, "yyyy-MM-dd");
    }

    public static String formatDateAsNumber(Date date) {  
        return formatDate(date, "yyyyMMddHHmmss");
    }

    public static String formatDate(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    // parses a string in the same format as given by formatNormalDate
    public static Date parseDate(String date) throws java.text.ParseException {
        if(date == null) return null;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(date);

        } catch(ParseException e) {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

            try {
                return sdf.parse(date);

            } catch(ParseException e2) {
                sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.parse(date);
            }
        }            
    }

    // changes a date with some timezone to a date with the local timezone
    public static Date changeTimezoneToLocal(Date date, String srcTimezone) {
        return changeTimezone(date, DateTimeZone.forID(srcTimezone), DateTimeZone.getDefault());
    }
    // changes a date with the system timezone to a date with another timezone
    public static Date changeTimezoneFromLocal(Date date, String resultTimezone) {
        return changeTimezone(date, DateTimeZone.getDefault(), DateTimeZone.forID(resultTimezone));
    }
    public static Date changeTimezone(Date date, String srcTimezone, String resultTimezone) {
        return changeTimezone(date, DateTimeZone.forID(srcTimezone), DateTimeZone.forID(resultTimezone));
    }
    public static Date changeTimezone(Date date, DateTimeZone srcTimezone, DateTimeZone resultTimezone) {
        if(date == null) return null;
        DateTime srcDate = new DateTime(date).withZoneRetainFields(srcTimezone);
        return srcDate.withZone(resultTimezone).toLocalDateTime().toDateTime().toDate();    // not sure why I need to do toLocalDateTime().toDateTime() but seems to be neccessary
    }


    // null startDates or endDates are ignored (and will not cause this function to return false)
    public static boolean dateIsBetween(Date testDate, Date startDate, Date endDate) {
        if((startDate == null || testDate.after(startDate)) && (endDate == null || testDate.before(endDate))) {
            return true;
        }
        return false;
    }

    public static Date addDays(String date,int days) throws Exception{
        return addHours(OfferUtil.parseDate(date), days*24);
    }

    public static Date hoursFromNow(int hours) {
        return addHours(new Date(), hours);
    }

    public static Date addHours(Date date, int hours) {
        return addTime(date, Calendar.HOUR, hours);
    }

    public static Date secondsFromNow(int seconds) {
        return timeFromNow(Calendar.SECOND, seconds);
    }

    public static Date timeFromNow(int incrimentType, int timeIncriment) { // time should be a Calendar enum
        return addTime(new Date(), incrimentType, timeIncriment);
    }

    public static Date addTime(Date date, int incrimentType, int timeIncriment) { // time should be a Calendar enum
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(incrimentType, timeIncriment);
        return c.getTime();
    }

    public static Date roundDownToIncrimentOfXMinutes(Date date, int minutesToRoundTo) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int unroundedMinutes = calendar.get(Calendar.MINUTE);
        calendar.set(Calendar.MINUTE, unroundedMinutes/minutesToRoundTo*minutesToRoundTo);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar.getTime();
    }

    public static int randInt(int min, int max) {
        return (new Random()).nextInt(max - min + 1) + min;
    }

    public static class DisplayTypeFunctions {
        public static ThreadLocal<List<String>> flags = new ThreadLocal<List<String>>(); // ensure that if this is set, it remains set that way for the duration of when the display-type expression is evaluated
        public static ThreadLocal<Boolean> override = new ThreadLocal<Boolean>(); // overrides the return value of flag

        public static boolean flag(String flagName) {
            Boolean or = override.get();
            if(or != null)
                return or;
            return flags.get().contains(flagName);
        }

        /*public static boolean flag(String flagName, int maxTimesSet) {
            Boolean or = override.get();
            if(or != null)
                return or;
            return flags.get().contains(flagName);
        }
        */
    }

    private static Map<String, ExpressionEvaluator> compiledExpressions = new HashMap<String, ExpressionEvaluator>();
    public static ExpressionEvaluator compileDisplayTypeExpression(String expression) throws Throwable {
        ExpressionEvaluator ee = compiledExpressions.get(expression);
        if(ee != null) {  // if the expression has not been compiled and cached
            return ee;
        } else {
            ee = new ExpressionEvaluator();
                ee.setExpressionType(boolean.class); // return value
                ee.setParameters(new String[] { "whale", "payer", "fbDirectPayer","amountSpent"}, new Class[] { boolean.class, boolean.class, boolean.class, int.class});  // parameters
                ee.setExtendedClass(OfferUtil.DisplayTypeFunctions.class); // used to inject the methods of that class into the expression-space so you can use them directly
            ee.cook(expression); //ee.cook("c + var(d)");  // Compile the expression once; relatively slow.
            compiledExpressions.put(expression, ee);
            return ee;
        }
    }

    public static boolean evalDisplayTypeExpression(ExpressionEvaluator ee, boolean whale, boolean payer, boolean fbDirectPayer, int amountSpent, List<String> flags) throws Throwable {
        OfferUtil.DisplayTypeFunctions.flags.set(flags); // set the flags

        // Evaluate it with varying parameter values; very fast.
        return (Boolean) ee.evaluate(
            new Object[] {          // arguments
                whale,
                payer,
                fbDirectPayer,
                amountSpent
            }
        );
    }


    public static void logScribeEvent(Logger classLogger, String app, ScribeReasonEnum reason, String message) {
        logScribeEvent(classLogger, app, reason, classLogger.getName(), message, null);
    }
    public static void logScribeEvent(Logger classLogger, String app, ScribeReasonEnum reason, String context, String message) {
        logScribeEvent(classLogger, app, reason, context, message, null);
    }

    // context has been used for the originating class
    // this is named 'logScribeEvent' for now-legacy reasons - it no longer logs to scribe but is a thresholded log
    public static void logScribeEvent(Logger classLogger, String app, ScribeReasonEnum reason, String context, String message, String userId) {
        LogThreshold thresholds = LogThreshold.getThresholds(app, reason);

        String thresholdMessage = null;
        if(thresholds == null) {
            thresholdMessage = "Error doesn't have a threshold listing";
        } else {

            boolean exceededHourlyThreshold=false;  // assume false
            try {
                exceededHourlyThreshold = thresholds.getLastHourErrorCount(app, reason) > thresholds.getPerHourThreshold();  
            } catch(Throwable t) {
                logger.warn("Could not add error to threshold Queue for "+app+" and reason: "+reason, t);
            }

            if(exceededHourlyThreshold) {
                thresholdMessage = "Error exceeded hourly threshold "+thresholds.getPerHourThreshold();
                thresholds.resetCounts(app, reason); // placing this call as close to the front as possible (so excuse the code duplication)
            } else if(thresholds.getAllTimeCount(app, reason) > thresholds.getThreshold()) {
                thresholdMessage = "Error exceeded threshold "+thresholds.getThreshold();
                thresholds.resetCounts(app, reason); // placing this call as close to the front as possible (so excuse the code duplication)
            } else {
                if( ! ServerCategory.isProduction() || thresholds.getAllTimeCount(app, reason)%20 == 0) { // only log sometimes
                    classLogger.info("Scribe Warning for app "+app+" and reason "+reason+": "+ reason.getReasonString() + " \n " + message);
                }

                thresholds.addErrorToQueue(app, reason);
            }
        }

        if(thresholdMessage != null) {
            classLogger.warn(thresholdMessage+" for app "+app+" and reason "+reason+": "+reason.getReasonString()+". Last message: "+message);
        }
    }

    public static void logScribeWarning(Logger classLogger, ScribeReasonEnum reason, String warning, HttpServletRequest req, IframeId iframeId, String userId) {
        logScribeWarning(classLogger, reason, warning, req, iframeId, userId, null);
    }
    public static void logScribeWarning(Logger classLogger, ScribeReasonEnum reason, String warning, HttpServletRequest req, IframeId iframeId, String userId, Throwable t) {
        String message = warningMessage(warning, req, iframeId, userId, t);
        String app=null;
        if(iframeId != null) { app = iframeId.getApp(); }
        logScribeEvent(classLogger, app, reason, classLogger.getName(), message, userId);
    }

    public static void warn(Logger classLogger, String warning, HttpServletRequest req, IframeId iframeId, String userId) {
        warn(classLogger, warning, req, iframeId, userId, null);
    }
    public static void warn(Logger classLogger, String warning, HttpServletRequest req, IframeId iframeId, String userId, Throwable t) {
        String message = warningMessage(warning, req, iframeId, userId);

        if(t != null) {
            classLogger.warn(message, t);
        } else {
            classLogger.warn(message);
        }
    }
    public static void error(Logger classLogger, String warning, HttpServletRequest req, IframeId iframeId, String userId) {
        warn(classLogger, warning, req, iframeId, userId, null);
    }
    public static void error(Logger classLogger, String warning, HttpServletRequest req, IframeId iframeId, String userId, Throwable t) {
        String message = warningMessage(warning, req, iframeId, userId);

        if(t != null) {
            classLogger.error(message, t);
        } else {
            classLogger.error(message);
        }
    }

    public static void info(Logger classLogger, String warning, HttpServletRequest req, IframeId iframeId, String userId, Throwable t) {
        String message = warningMessage(warning, req, iframeId, userId);

        if(t != null) {
            classLogger.info(message, t);
        } else {
            classLogger.info(message);
        }
    }

    private static String warningMessage(String warning, HttpServletRequest req, IframeId iframeId, String userId) {
        return warningMessage(warning, req, iframeId, userId, null);
    }
    private static String warningMessage(String warning, HttpServletRequest req, IframeId iframeId, String userId, Throwable t) {
        String message = warning;

        if(iframeId != null) { message += ".\n IframeId: "+iframeId; }
        if(userId != null) { message += ".\n UserId: "+userId; }
        if(req != null) { message += ".\n Headers and Request: "+TrackAction.dumpRequestAndHeaders(req); }
        if(t != null) { message += ".\n Exception: "+OfferUtil.realThrowableToString(t); }

        return message;
    }



    public static void logErrorIfTooManyWarnings(int warningsPerHundred) {
        // connect to logs db

        // get number of warnings in the last hour
        //    "SELECT * FROM livelog where datetime > DATE_SUB(NOW(), INTERVAL 1 HOUR)"
        // calculate ratio

        // error if ratio is too high
    }

    public static <T extends Object> Map<T, T> toMap(T[][] items) {
        Map<T, T> result = new HashMap<T, T>();
        for(T[] x : items) {
            result.put(x[0], x[1]);
        }
        return result;
    }

    public static <T extends Object> Map<T, T[]> toArrayMap(T[][] items) {
        Map<T, T[]> result = new HashMap<T, T[]>();
        for(T[] x : items) {
            result.put(x[0], (T[]) ArrayUtils.subarray(x, 1, x.length));
        }
        return result;
    }

    public static <T extends Object> Map<Pair<T,T>, T[]> toPairArrayMap(T[][] items) {
        Map<Pair<T,T>, T[]> result = new HashMap<Pair<T,T>, T[]>();
        for(T[] x : items) {
            result.put(Pair.newPair(x[0],x[1]), (T[]) ArrayUtils.subarray(x, 2, x.length));
        }
        return result;
    }

    public static BigDecimal roundToPoint99(BigDecimal x) {
        //BigDecimal above = x.add(x.round(new MathContext(x.precision() - endsWith.precision(), RoundingMode.CEILING)));
        BigDecimal p49 = new BigDecimal(".49");
        BigDecimal p99 = new BigDecimal(".99");
        BigDecimal xModOne = x.remainder(new BigDecimal(1));

        // if the remainder is above or equal to .49, round up
        if(xModOne.compareTo(p49) >= 0 ) {
            return x.add(p99.subtract(xModOne)).stripTrailingZeros();
        } else {
            return x.subtract(xModOne).subtract(new BigDecimal(".01")).stripTrailingZeros();
        }
    }

    public static BigDecimal roundToPrecision(int precision, BigDecimal x) {
        if (precision == 0) {
            return x.setScale(0,BigDecimal.ROUND_DOWN);
        } else {
            return x.round(new MathContext(precision));
        }
    }

    public static BigDecimal newBigDecimal(String constructorValue) {
        if(constructorValue == null) return null;
        return new BigDecimal(constructorValue);
    }



    public static String getTruncatedString(String s, int length) {
        if (s == null) return null;

        if (s.length() <= length) return s;

        return s.substring(0, length);
    }

    public static String realThrowableToString(Throwable t) {
        Writer writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        t.printStackTrace(printWriter);

        return t.getMessage()+" "+writer.toString();
    }

    public static String listToString(List x) {
        String result="[";
        boolean onceAlready = false;
        for (Object item : x) {
            if(onceAlready) result += ", ";
            else onceAlready = true;

            if(item instanceof List) {
                result += listToString((List)item);
            } else if(item instanceof Map) {
                result += mapToString((Map)item);
            } else if(item != null && item.getClass().isArray()) {
                result += Arrays.toString((Object[])item);
            } else {
                result += item.toString();
            }
        }
        return result+"]";
    }

    public static String mapToString(Map x) {
        String result="{";
        boolean onceAlready = false;
        for (Object n : x.keySet()) {
            Object item = x.get(n);

            if(onceAlready) result += ", ";
            else onceAlready = true;

            result += n+": ";

            if(item == null) {
                result += "null";
            } else if(item instanceof List) {
                result += listToString((List)item);
            } else if(item instanceof Map) {
                result += mapToString((Map)item);
            } else if(item.getClass().isArray()) {
                result += Arrays.toString((Object[])item);
            } else {
                result += item.toString();
            }
        }
        return result+"}";
    }


    public static interface Goable<ReturnType, ParameterType> {
        public ReturnType go(ParameterType parmeter) throws Throwable;
    }

    // runnable with a parameter, return value, and exceptions
    public static class GeneralRunnable<ReturnType, ParameterType> implements Runnable {
        Throwable exception = null;
        ReturnType result;
        Goable<ReturnType, ParameterType> goable;
        ParameterType parameter;
        Boolean done = null; // null means not even started

        public GeneralRunnable(Goable<ReturnType, ParameterType> goable) {
            this.goable = goable;
            this.parameter = null;
        }

        public void setParameter(ParameterType p) {
            this.parameter = p;      
        }

        public void run() {
            done = false;
            try {
                result = goable.go(parameter);
            } catch (Throwable e) {
                exception = e;
            }
            done = true;
        }

        public ReturnType getResult() throws Throwable {
            if( ! done) {
                throw new RuntimeException("Tried to getResult from GeneralRunnable before it was finished.");
            }

            if(exception != null) {
                addCause(exception);
                throw exception;
            }
            return result;
        }
    }

    public static class Timeout {
        public static class Exception extends RuntimeException {
            Exception(String msg) {
                super(msg);
            }
        }

        public static <T> T execute(int timeoutInMilliseconds, final Goable<T, Object> g) throws Throwable {
            GeneralRunnable<T, Object> r = new GeneralRunnable<T, Object>(g); 

            Thread serviceThread = new Thread(r);
            serviceThread.start();

            synchronized (r) { try {
                serviceThread.join(timeoutInMilliseconds);
            } catch (InterruptedException exc) {/* call has timed out (most likely) */}}
            if (serviceThread.isAlive()) {
                serviceThread.interrupt();
                throw new Timeout.Exception("Call took longer than specified timeout "+timeoutInMilliseconds);
            }

            return r.getResult();
        }
    }

    public static <T extends ObjectWithIframeId> Map<IframeId, T> buildCacheMapByIframeId(List<T> rowList) {
        Map<IframeId, T> newMap = new HashMap<IframeId, T>();
        for (T entry : rowList) {
            if(newMap.containsKey(entry.getIframeId())) {
                logger.warn("More than one entry found for iframeId - buildCacheMapByIframeId assumes there will only be one row per iframeId. "+entry+ "Called from: " + Thread.currentThread().getStackTrace());
            }
            newMap.put(entry.getIframeId(), entry);
        }
        return newMap;
    }

    public static <T extends ObjectWithIframeId> Map<IframeId, List<T>> buildCacheMapByIframeId_list(List<T> rowList) {
        return buildCacheMapByIframeId_list(rowList, null);
    }

    // the callback r is run for every entry and gets passed the entry, and the map in a Pair object
    public static <T extends ObjectWithIframeId> Map<IframeId, List<T>> buildCacheMapByIframeId_list(List<T> rowList, RunnableWithParameter r) { 
        Map<IframeId, List<T>> newMap = new HashMap<IframeId, List<T>>();
        for (T entry : rowList) {
            IframeId id = entry.getIframeId();
            if(newMap.get(id) == null) {
                newMap.put(id, new ArrayList<T>());
            }
            newMap.get(id).add(entry);
            if(r != null) { r.run(Pair.newPair(newMap, entry)); }
        }
        return newMap;
    }

    

    // adds a new RunTimeException onto the deepest cause to track the stack trace through rethrows
    // NOTE - I'm fairly certain java doesn't allow doing what I intended this to do. 
    public static void addCause(Throwable t) {
        addCause(t, new RuntimeException());
    }
    public static void addCause(Throwable e, Throwable exceptionToAttach) {
        // eff it, java won't let me do this right - causes will appear backwards sometimes (ie the rethrow will look like it came before the cause) 
        Throwable c = e;
        while(true) {
            if(c.getCause() == null) {
                break;
            }
            //else
            c = c.getCause();        // get cause here will most likely return null : ( - which means I can't do what I wanted to do
        }

        try {
            c.initCause(exceptionToAttach);
        } catch (Throwable t) {
            OfferUtil.warn(logger,"Unable to attach cause because " + t.getMessage() + ". Throwing original exception.",null,null,null,e);
        }
    }

    public static RuntimeException wrapException(String message, Throwable t) {
        RuntimeException e = new RuntimeException(message);
        e.initCause(t);
        return e;
    }


    public static <T> void addToIframeListMap(IframeId iframeId, Map<IframeId, List<T>> map, T newOne) {
        List<T> newOnes = new ArrayList<T>();
        newOnes.add(newOne);
        addToIframeListMap(iframeId, map, newOnes);
    }

    public static <T> void addToIframeListMap(IframeId iframeId, Map<IframeId, List<T>> map, List<T> newOnes) {
        if(map.get(iframeId) == null) map.put(iframeId, new ArrayList<T>());
        map.get(iframeId).addAll(newOnes);
    }

    public static class RollingCount<KeyType> {

        private Map<KeyType, LinkedBlockingQueue<Date>> counts = new HashMap<KeyType, LinkedBlockingQueue<Date>>();
        int rollingPeriodInMinutes;
        Calendar calendarObject;

        public RollingCount(int minutes) {
            calendarObject = Calendar.getInstance();
            rollingPeriodInMinutes = minutes;
        }

        public int getCount(KeyType key) throws InterruptedException {
            purgeErrorsOutsideRollingPeriod(key);

            LinkedBlockingQueue list = counts.get(key);
            if(list == null) {
                return 0;
            } else {
                return list.size();
            }
        }

        public void addToQueue(KeyType key) {
            LinkedBlockingQueue<Date> list = counts.get(key);

            if(list == null) {
                list = new LinkedBlockingQueue<Date>();
                counts.put(key, list);
            }
            list.add(new Date());
        }
        public void resetCounts(KeyType key) {
            counts.put(key, new LinkedBlockingQueue<Date>());
        }
        private void purgeErrorsOutsideRollingPeriod(KeyType key) throws InterruptedException {
            calendarObject.setTime(new Date());
            calendarObject.add(Calendar.MINUTE, -rollingPeriodInMinutes);

            LinkedBlockingQueue<Date> list = counts.get(key);
            if(list != null) {
                while(list.peek() != null && list.peek().before(calendarObject.getTime())) {
                    list.take();
                }
            }
        }
    }

    // keeps a percentage that's purged every
    public static class TimedPercentCount {
        public int numerator, denominator;
        int minutes;
        Date nextPurge;

        public TimedPercentCount(int minutes) {
            this.minutes = minutes;
            reset();
        }

        public void incDenominator() {
            purgeIfNeccessary();
            denominator++;

        }
        public void incNumerator() {
            purgeIfNeccessary();
            numerator++;
        }

        public float getPercent() {
            return numerator/denominator;
        }

        public void reset() {
            numerator = 1;
            denominator = 1;
            nextPurge = timeFromNow(Calendar.MINUTE, minutes);
        }
        private void purgeIfNeccessary() {
            if(nextPurge.before(new Date())) {  // if now is later than the next purge date
                reset();
            }
        }
    }

    public static class ErrorCooldownCounter {
        private OfferUtil.TimedPercentCount errorRate = null;
        private Date coolDownReleaseDate = null;   // null is the normal state - ready to use, not cooling down
        int coolDownMinutes, minFailures;
        float maxPercentage;

        public ErrorCooldownCounter(int coolDownMinutes, int minFailures, float maxPercentage) {
            if(maxPercentage > 1) {
                throw new RuntimeException("maxPercentage should be a number between 0 and 1");
            }
            this.minFailures = minFailures;
            this.coolDownMinutes = coolDownMinutes;
            this.maxPercentage = maxPercentage;
        }

        // checks if you can attempt
        // returns false if its cooling down
        // returns true if you can go ahead
        // records an attempt if you can attempt
        public boolean attempt() {
            if(coolDownReleaseDate != null) {
                if(coolDownReleaseDate.before(new Date())) {
                    coolDownReleaseDate = null;
                    errorRate = null;
                } else {
                    return false;
                }
            }
            // else
            initErrorRateIfNeccessary();
            errorRate.incDenominator();  // another game service attempt
            return true;
        }

        public void failure(String message) {
            failure(message, true);   
        }

        public void failure(String message, boolean printStackTrace) {
            initErrorRateIfNeccessary();
            errorRate.incNumerator();

            // need at least 10 failures to trigger this
            if(errorRate.getPercent() > maxPercentage && errorRate.denominator > minFailures) {
                coolDownReleaseDate = OfferUtil.timeFromNow(Calendar.MINUTE, coolDownMinutes);  // cool down for 2 minutes
                logger.warn("Cooling down "+message+" for "+coolDownMinutes+" minutes. "+
                            "Currently there are "+errorRate.numerator+" errors out of "+errorRate.denominator+" attempts."
                            +(printStackTrace?realThrowableToString(new Throwable()):""));
            }
        }

        private void initErrorRateIfNeccessary() {
            if(errorRate == null) errorRate = new OfferUtil.TimedPercentCount(2);  // reset the error rate every two minutes
        }
    }

    public static class KeyedErrorCooldownCounter<KeyType> {
        private Map<KeyType, ErrorCooldownCounter> counter = new HashMap<KeyType,ErrorCooldownCounter>();
        int coolDownMinutes, minFailures;
        float maxPercentage;

        public KeyedErrorCooldownCounter(int coolDownMinutes, int minFailures, float maxPercentage) {
            if(maxPercentage > 1) {
                throw new RuntimeException("maxPercentage should be a number between 0 and 1");
            }
            this.minFailures = minFailures;
            this.coolDownMinutes = coolDownMinutes;
            this.maxPercentage = maxPercentage;
        }

        // checks if you can attempt
        // returns false if its cooling down
        // returns true if you can go ahead
        // records an attempt if you can attempt
        public boolean attempt(KeyType key) {
            if(counter.get(key) == null) {
                counter.put(key, new ErrorCooldownCounter(coolDownMinutes, minFailures, maxPercentage));
            }

            return counter.get(key).attempt();
        }

        public void failure(KeyType key) {
            counter.get(key).failure("key "+key);
        }
    }


    // Cache control: http://www.jguru.com/faq/view.jsp?EID=377
    public static void cachePrevention(HttpServletResponse resp) {
        resp.setHeader("Cache-Control","no-cache"); //HTTP 1.1
        resp.setHeader("Pragma","no-cache"); //HTTP 1.0
        resp.setDateHeader ("Expires", 0); //prevents caching at the proxy server
    }
}
