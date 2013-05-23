package com.util;

import com.offers.util.IframeId;
import com.offers.util.Cept;
import com.offers.IpRange;
import com.offers.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONException;

import javax.servlet.http.HttpServletRequest;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Mac;
import javax.crypto.Cipher;
import java.util.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.security.Key;

import org.apache.commons.codec.binary.Base64;


/**
 * Created by IntelliJ IDEA.
 * User: Chris Wang
 * Date: Jul 18, 2008
 * Time: 4:27:58 PM
 */
public class SecurityUtil {

    // don't use this directly - cache this somehow
    // currently its cached in Transaction.PartnerEnum as the member privateSecret
    public static String getPartnerPrivateSecret(Transaction.PartnerEnum partner) {
        try {
            return new String(Base64.encodeBase64(DigestUtils.md5(partner.name()+ SecurityUtil.getSecretMonetizationKey())), "UTF-8");
        } catch(Throwable t) {
            throw new Cept("oops",t);
        }
    }

    // returned encrypted user IDs always begin with an underscore (_)
    public static String obfusctateUserId(String userId, Transaction.PartnerEnum partner) {
        return "_"+cryptonateRC4(Cipher.ENCRYPT_MODE, getPartnerPrivateSecret(partner), userId);
    }
    public static String unobfuscateUserId(String obfuscatedUserId, Transaction.PartnerEnum partner) {
        String encryptedUserId = obfuscatedUserId.substring(1);  // remove underscore
        return cryptonateRC4(Cipher.DECRYPT_MODE, partner.getPrivateSecret(), encryptedUserId);
    }
    // returns an RC4 encrypted, base64 encoded ciphertext
    // message and secret should be UTF-8 encoded
    // cipherMode should either be Cipher.DECRYPT_MODE or Cipher.ENCRYPT_MODE
    private static String cryptonateRC4(int cipherMode, String secret, String message) {
        try {
            Key key = new SecretKeySpec(secret.getBytes("UTF-8"), "RC4");
            Cipher encryptor = Cipher.getInstance("RC4");
            encryptor.init(cipherMode, key);

            byte[] output;
            if(cipherMode == Cipher.ENCRYPT_MODE) {
                output = Base64.encodeBase64(encryptor.doFinal(message.getBytes("UTF-8")));
            } else { // decrypt mode
                output = encryptor.doFinal(Base64.decodeBase64(message));
            }

            return new String(output, "UTF-8");

        } catch(Throwable t) {
            throw new Cept("Problem encrypting or decrypting",t);
        }
    }




    // parameters could be secured with a hash parameter (h) optionally.
    // This hash would be computed as follows:
    // sort all passed parameters by name
    // compute md5 hash of all (param_name + param_value) in sorted order + monetization secret key
    // If the hash is a mismatch, we would ignore all params except the minimal required.
    public static void validateHash(HttpServletRequest req, String[] secureParameters, String monetizationKey) {
        Map<String, String> parameters = getParameters(req);
        boolean secureParameterExists = false;
        for(String key : parameters.keySet()) {
            if(Arrays.asList(secureParameters).contains(key)) {
                secureParameterExists = true;
            }
        }

        if(secureParameterExists) {    // only if parameters that need to be secured are passed
            String theirHash = req.getParameter("h");
            if(theirHash == null)
                throw new RuntimeException("Hash is required, but is not supplied");

            validateHash(parameters, theirHash, monetizationKey);
        }
    }
    public static void validateHash(Map<String, String> parameters, String theirHash, String key) {
        String ourHash = calculateHash(parameters, key);
        boolean hashesMatch = ourHash.equalsIgnoreCase(theirHash);
        if (!hashesMatch)
            throw new RuntimeException("Iframe hash mismatch between our calculated " + ourHash + " and the given hash " + theirHash);
    }

    public static Map<String, String> getParameters(HttpServletRequest req) {
        // create parameter map from the request
        // assumes that there is only one value per key (ie parameters only have one value and are not lists)
        Map<String, String> parameters = new HashMap<String, String>();
        for(Object key : req.getParameterMap().keySet()) {
            String keyString = (String) key;
            parameters.put(keyString, req.getParameter(keyString));
        }

        return parameters;
    }

    public static String calculateHash(HttpServletRequest req, String key) {
         Map<String, String> parameters = getParameters(req);
        return calculateHash(parameters, key);
    }
    public static String calculateHash(Map<String, String> parameters, String key) {
        List paramNames = Arrays.asList(parameters.keySet().toArray());

        Collections.sort(paramNames);
        StringBuilder s = new StringBuilder();
        for (Object param : paramNames) {
            String paramName = (String) param;
            if(!"h".equals(paramName)) {    // as long as its not the hash parameter
                s.append(paramName).append( (String) parameters.get(paramName));
            }
        }
        s.append(key);

        return DigestUtils.md5Hex(s.toString());
    }


    // hmac based on RFC 2104 (unsure if this is true anymore)
    // returns base64 encoded string
    public static String sha1HMAC(String key, String message) throws Throwable {
        SecretKeySpec spec = new SecretKeySpec(
            key.getBytes(),
            "HmacSHA1");

        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(spec);

        byte[] result = mac.doFinal(message.getBytes());
        byte[] empty = new byte[]{};  // don't do line separaters..
        Base64 encoder = new Base64(0,  empty);  // and don't chunk the result furcrisake

        return encoder.encodeToString(result);
    }

    public static void validatePlaydomInternalIp(HttpServletRequest req) {
        if( ! IpRange.inList(req, "playdomInternal")) {
            throw new RuntimeException("Invalid IP address - not playdom internal");
        }
    }



    /*
    // Test if the request pass security check, if appData is null, the security check code will test all known app data
    public static boolean securityCheckWithSmallFalsePositive(HttpServletRequest req, Collection<ApplicationData> appData) {
        // Sometimes the opensocial_view_id does not match the user's id
        // Sometimes the signature will fail, but it's very rare
        return checkConsumerKey(req, appData) && checkOauthSignature(req, appData);
    }
    */

    // This method assumes that checkConsumerKey returns true
    /*@SuppressWarnings("unchecked")
    public static boolean checkOauthSignature(HttpServletRequest req, Collection<ApplicationData> appData) {
        Map<String, String[]> paramMap = req.getParameterMap();
        List<String> argList = new ArrayList<String>();

        for (String key : paramMap.keySet()) {
            if (!key.equals("oauth_signature")) {
                String[] temp_val = paramMap.get(key);

                if (temp_val.length > 0) {
                    for (String aTemp_val : temp_val) {
                        argList.add(key + "=" + RestAPIMySpace.encodeWrapper(aTemp_val));
                    }
                } else {
                    argList.add(key +"=");
                }
            }
        }

        Collections.sort(argList);

        StringBuilder part3 = new StringBuilder();
        for (int i = 0; i < argList.size(); i++) {
            part3.append(argList.get(i));
            if (i != argList.size()-1) {
                part3.append("&");
            }
        }

        String part1 = req.getMethod();
        String part2 = req.getRequestURL().toString();
        String baseString = RestAPIMySpace.encodeWrapper(part1) + "&" + RestAPIMySpace.encodeWrapper(part2) + "&" + RestAPIMySpace.encodeWrapper(part3.toString());

        String oauth_signature = FBForm.getParameterValueString(req, "oauth_signature");
        for (ApplicationData data : ApplicationData.values()) {
            if (appData != null && !appData.contains(data)) continue;
            String sig = RestAPIMySpace.getHMACSHA1(data.getSecretKey() + "&", baseString);
            if (sig.equals(oauth_signature)) {
                StatAction.addStat("Oauth_success");
                return true;
            }
        }

        StatAction.addStat("Oauth_fail_due_to_bad_signature");
        return false;
    }


    public static boolean checkConsumerKey(HttpServletRequest req, Collection<ApplicationData> appData) {
        String oauth_consumer_key = FBForm.getParameterValueString(req, "oauth_consumer_key");
        if (oauth_consumer_key == null) {
            StatAction.addStat("Oauth_fail_due_to_null_consumer_key");
            return false;
        }

        // Secret word to by pass this check
        if ("password".equals(oauth_consumer_key)) return true;

        for (ApplicationData data : ApplicationData.values()) {
            if (appData != null && !appData.contains(data)) continue;

            if (data.getConsumerKey().equals(oauth_consumer_key)) {
                if (data.isUserIdIsAlwaysViewer()) {
                    String user_id = FBForm.getParameterValueLong(req, "user_id");
                    Long opensocial_viewer_id = FBForm.getParameterValueLong(req, "opensocial_viewer_id");
                    if (user_id == null || user_id.equals(opensocial_viewer_id)) {
                        return true;
                    } else {
                        StatAction.addStat("Oauth_fail_due_to_bad_viewer");
                        return false;
                    }
                } else {
                    return true;
                }
            }
        }
        StatAction.addStat("Oauth_fail_due_to_unknown_consumer_key");
        return false;
    }
    */

}
