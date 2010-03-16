/**
 * Copyright 2010 TransPac Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Based on public domain versin released by aw2.0 Ltd
 *     http://www.aw20.co.uk/
 */
package com.bixolabs.aws;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class SimpleDB {
    public static final String TIMESTAMP_METADATA = "Timestamp";
    
    private static final String SIGNATURE_METHOD = "HmacSHA1";
    private static final String API_VERSION = "2009-04-15";
    private static final String SIGNATURE_VERSION = "2";
    private static final String DEFAULT_HOST = "sdb.amazonaws.com";

    /**
     * Implementation of IHttpHandler based on java.net.HttpURLConnection class.
     * 
     * This will not handle proxies, or retries, or backing off when there are
     * 503 responses from Amazon.
     */
    private static class SimpleHttpHandler implements IHttpHandler {

        @Override
        public String get(URL url) throws IOException, HttpException, InterruptedException {
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            checkResponse(con);
            return getString(con.getInputStream());
        }

        @Override
        public String post(URL url, Map<String, String> params) throws IOException, HttpException, InterruptedException {
            StringBuilder error = new StringBuilder("url: " + url + "\n");
            
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("POST");
            
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setUseCaches(false);
            con.setAllowUserInteraction(false);
            con.setRequestProperty("Host", url.getHost());
            con.setRequestProperty("Content-type", "application/x-www-form-urlencoded");
            
            /* Send out the data */
            OutputStream out = con.getOutputStream();
            Writer writer = new OutputStreamWriter(out, "UTF-8");
            
            try {
                List<String> keys = new ArrayList<String>(params.keySet());
                Collections.sort(keys);
                
                Iterator<String> it = keys.iterator();
                while (it.hasNext()) {
                    String key = it.next();
                    String val = params.get(key);
        
                    error.append("\tKey = " + key + ", value = " + val);
                    error.append('\n');
                    
                    writer.write(key);
                    writer.write("=");
                    writer.write(URLEncoder.encode(val, "utf-8").replace("+", "%20").replace("*", "%2A").replace("%7E","~"));

                    if (it.hasNext()) {
                        writer.write("&");
                    }
                }
            } finally {
                writer.flush();
                writer.close();
            }

            try {
                checkResponse(con);
                // System.out.println("Posted valid " + error.toString());
            } catch (HttpException e) {
                System.out.println("Posted invalid " + error.toString());
                throw e;
            }
            
            return getString(con.getInputStream());
        }
        
        private void checkResponse(HttpURLConnection con) throws IOException, HttpException {
            int statusCode = con.getResponseCode();
            if (statusCode >= 300) {
                String response = getString(con.getErrorStream());
                throw new HttpException(statusCode, response);
            }
        }

        private String getString(InputStream is) throws IOException {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte ba[] = new byte[8192];
                int read = is.read(ba);
                while (read > -1) {
                    out.write(ba, 0, read);
                    read = is.read(ba);
                }
                
                return out.toString("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Impossible exception" + e);
            } finally {
                try {
                    is.close();
                } catch (Exception e){}
            }
        }
    }
    
    /**
     * Implementation of IXmlParser based on simple string matching.
     * 
     */
    private static class SimpleXmlParser implements IXmlParser {

        @Override
        public List<String> getElements(String text, String elem) {
            List<String> list = new ArrayList<String>();

            String sTag = "<" + elem + ">";
            String eTag = "</" + elem + ">";

            int c1 = text.indexOf(sTag);
            int c2 = text.indexOf(eTag);
            while (c1 != -1 && c2 != -1) {
                list.add(text.substring(c1 + sTag.length(), c2));
                c1 = text.indexOf(sTag, c2);
                if (c1 != -1)
                    c2 = text.indexOf(eTag, c1);
            }
            
            return list;
        }
        
        @Override
        public String getElement(String text, String elementName) {
            List<String> elements = getElements(text, elementName);
            if (elements.size() > 0) {
                return elements.get(0);
            } else {
                return null;
            }
        }
    }
    
    private String _httpEndPoint;
    private String _awsId;
    private SecretKeySpec secret;
    private IHttpHandler _httpHandler;
    private IXmlParser _xmlParser;
    
    private Mac _mac = null;
    
    private String  lastRequestId = null;
    private String  lastBoxUsage = null;
    private String  lastToken = null;

    public SimpleDB(String awsId, String secretKey) {
        this("sdb.amazonaws.com", awsId, secretKey);
    }
    
    public SimpleDB(String awsId, String secretKey, IHttpHandler httpHandler) {
        this(DEFAULT_HOST, awsId, secretKey, httpHandler);
    }
    
    public SimpleDB(String host, String awsId, String secretKey) {
        this(host, awsId, secretKey, new SimpleHttpHandler());
    }

    public SimpleDB(SimpleDB original) {
        _httpEndPoint = original._httpEndPoint;
        _awsId = original._awsId;
        _httpHandler = original._httpHandler;

        this.secret = original.secret;
        
        _xmlParser = original._xmlParser;
    }
    
    public SimpleDB(String host, String awsId, String secretKey, IHttpHandler httpHandler) {
        _httpEndPoint = host;
        _awsId = awsId;
        _httpHandler = httpHandler;

        this.secret = new SecretKeySpec(secretKey.getBytes(), SIGNATURE_METHOD);
        
        _xmlParser = new SimpleXmlParser();
    }

    public String getLastRequestId(){
        return lastRequestId;
    }

    public String getLastBoxUsage(){
        return lastBoxUsage;
    }

    public String getLastToken(){
        return lastToken;
    }
    
    
    /*
     * The CreateDomain operation creates a new domain. The domain name must be unique among the domains associated 
     * with the Access Key ID provided in the request. The CreateDomain operation might take 10 or more seconds to complete.
     * 
     * Returns the domain that was created
     */
    public String createDomain(String domainName) throws IOException, AWSException, InterruptedException {
        Map<String, String>  uriParams = createStandardParams("CreateDomain");
        uriParams.put("DomainName", domainName);
        uriParams.put("Signature", getSignature(uriParams));
        
        doSimpleGet(uriParams);
        return domainName;
    }

    
    /*
     * The ListDomains operation lists all domains associated with the Access Key ID. It returns domain names up to the 
     * limit set by MaxNumberOfDomains. A NextToken is returned if there are more than MaxNumberOfDomains domains. Calling 
     * ListDomains successive times with the NextToken returns up to MaxNumberOfDomains more domain names each time.
     */
    public List<String> listDomains() throws IOException, AWSException, InterruptedException {
        return listDomains(-1, null);
    }
    
    public List<String> listDomains(int maxNumberOfDomains, String nextToken) throws IOException, AWSException, InterruptedException {
        Map<String, String> uriParams = createStandardParams("ListDomains");
        
        if (maxNumberOfDomains > 0) {
            uriParams.put("MaxNumberOfDomains", String.valueOf(maxNumberOfDomains));
        }
        
        if (nextToken != null) {
            uriParams.put("NextToken", nextToken);
        }
        
        uriParams.put("Signature", getSignature(uriParams));
        return _xmlParser.getElements(doSimpleGet(uriParams), "DomainName");
    }


    /*
     * The DeleteDomain operation deletes a domain. Any items (and their attributes) in the domain 
     * are deleted as well. The DeleteDomain operation might take 10 or more seconds to complete.
     * 
     * returns back the domainName we just deleted
     */
    public String deleteDomain(String domainName) throws IOException, AWSException, InterruptedException {
        Map<String, String> uriParams = createStandardParams("DeleteDomain");
        uriParams.put("DomainName", domainName);
        uriParams.put("Signature", getSignature(uriParams));
        
        doSimpleGet(uriParams);
        return domainName;
    }

    
    

    /*
     * Returns information about the domain, including when the domain was created,
     * the number of items and attributes, and the size of attribute names and values.
     * 
     * returns back a map of key/value properties about this domain
     */
    public Map<String, String> domainMetaData(String domainName) throws IOException, AWSException, InterruptedException {
        Map<String, String> uriParams = createStandardParams("DomainMetadata");
        uriParams.put("DomainName", domainName);
        uriParams.put("Signature", getSignature(uriParams));
        
        String resp = doSimpleGet(uriParams);
        
        Map<String, String> el = new HashMap<String, String>();
        
        putIfElementExists(el, resp, "Timestamp");
        putIfElementExists(el, resp, "ItemCount");
        putIfElementExists(el, resp, "AttributeValueCount");
        putIfElementExists(el, resp, "AttributeNameCount");
        putIfElementExists(el, resp, "ItemNamesSizeBytes");
        putIfElementExists(el, resp, "AttributeValuesSizeBytes");
        putIfElementExists(el, resp, "AttributeNamesSizeBytes");
        
        return el;
    }

    private void putIfElementExists(Map<String, String> values, String resp, String element) {
        String value = _xmlParser.getElement(resp, element);
        if (value != null) {
            values.put(element, value);
        }
    }

    /*
     * With the BatchPutAttributes operation, you can perform multiple PutAttribute 
     * operations in a single call. This helps you yield savings in round trips and 
     * latencies, and enables Amazon SimpleDB to optimize requests, which generally 
     * yields better throughput.
     */
    public String batchPutAttributes(String domainName, Map<String, Map<String,String>> itemValues) throws AWSException, IOException, InterruptedException {
        return batchPutAttributes(domainName, itemValues, new HashMap<String, Set<String>>());
    }
    
    public String batchPutAttributes(String domainName, Map<String, Map<String,String>> itemValues, Map<String, Set<String>> itemReplaces) throws AWSException, IOException, InterruptedException  {
        Map<String, String> uriParams = createStandardParams("BatchPutAttributes");
        uriParams.put("DomainName", domainName);
        
        int itemCount = 0;
        for (Map.Entry<String, Map<String,String>> itemMap : itemValues.entrySet()) {
            
            int count = 0;
            Map<String,String> map = itemMap.getValue();
            Set<String> replace = itemReplaces.get(itemMap.getKey());
            
            uriParams.put("Item." + itemCount + ".ItemName", itemMap.getKey());
            
            for (Map.Entry<String, String> x : map.entrySet()) {
                uriParams.put("Item." + itemCount + ".Attribute." + count + ".Name", x.getKey());
                uriParams.put("Item." + itemCount + ".Attribute." + count + ".Value", x.getValue());

                if (replace != null && replace.contains(x.getKey()))
                    uriParams.put("Item." + itemCount + ".Attribute." + count + ".Replace", "true");

                ++count;
            }

            ++itemCount;
        }
        
        uriParams.put("Signature", getSignature(false, _httpEndPoint, uriParams));
        doSimplePost(uriParams);

        return domainName;
    }

    
    
    
    /*
     * The PutAttributes operation creates or replaces attributes in an item. You specify new 
     * attributes using a combination of the Attribute.X.Name and Attribute.X.Value parameters. 
     * You specify the first attribute by the parameters Attribute.0.Name and Attribute.0.Value, 
     * the second attribute by the parameters Attribute.1.Name and Attribute.1.Value, and so on.
     */
    public String putAttributes(String domainName, String itemName, Map<String, String> map) throws IOException, AWSException, InterruptedException {
        return putAttributes(domainName, itemName, map, null);
    }
    
    public String putAttributes(String domainName, String itemName, Map<String, String> map, Set<String> replace) throws IOException, AWSException, InterruptedException {
        Map<String, String> uriParams = createStandardParams("PutAttributes");
        uriParams.put("DomainName", domainName);
        uriParams.put("ItemName", itemName);

        int count = 0;
        for (Map.Entry<String, String> x : map.entrySet()) {
            uriParams.put("Attribute." + count + ".Name", x.getKey());
            uriParams.put("Attribute." + count + ".Value", x.getValue());

            if (replace != null && replace.contains(x.getKey()))
                uriParams.put("Attribute." + count + ".Replace", "true");

            ++count;
        }

        uriParams.put("Signature", getSignature(false, _httpEndPoint, uriParams));
        doSimplePost(uriParams);

        return domainName;
    }


    /*
     * Deletes one or more attributes associated with the item. If all 
     * attributes of an item are deleted, the item is deleted.
     * 
     * If the value of the map is null, then all the attributes of that name will be deleted
     */
    public String deleteAttributes(String domainName, String itemName) throws IOException, AWSException, InterruptedException {
        return deleteAttributes(domainName, itemName, null);
    }   
    
    public String deleteAttributes(String domainName, String itemName, Map<String, String> map) throws IOException, AWSException, InterruptedException {
        Map<String, String> uriParams = createStandardParams("DeleteAttributes");
        uriParams.put("DomainName", domainName);
        uriParams.put("ItemName", itemName);

        if (map != null) {
            int count = 0;
            for (Map.Entry<String, String> x : map.entrySet()) {
                uriParams.put("Attribute." + count + ".Name", x.getKey());
    
                if (x.getValue() != null)
                    uriParams.put("Attribute." + count + ".Value", x.getValue());
    
                ++count;
            }
        }
        uriParams.put("Signature", getSignature(uriParams));
        doSimpleGet(uriParams);
        return domainName;
    }

    
    
    /*
     * Returns all of the attributes associated with the item. Optionally, the attributes 
     * returned can be limited to one or more specified attribute name parameters.
     * 
     * If the item does not exist on the replica that was accessed for this operation, 
     * an empty set is returned. The system does not return an error as it cannot 
     * guarantee the item does not exist on other replicas.
     * 
     * Returns a HashMap of key/String[]
     * 
     */
    private Map<String, String[]> getAttributes(String domainName, String itemName, String attributeName, boolean consistentRead) throws IOException, AWSException, InterruptedException {
        Map<String, String> uriParams = createStandardParams("GetAttributes");
        uriParams.put("DomainName", domainName);
        uriParams.put("ItemName", itemName);

        if (attributeName != null) {
            uriParams.put("AttributeName", attributeName);
        }
        
        if (consistentRead) {
            uriParams.put("ConsistentRead", "true");
        }
        
        uriParams.put("Signature", getSignature(uriParams));
        String resp = doSimpleGet(uriParams);
        
        Map<String, String[]> m = new HashMap<String, String[]>();
        
        List<String> attributes = _xmlParser.getElements(resp, "Attribute");
        for (int x = 0; x < attributes.size(); x++) {
            String t = attributes.get(x);

            // TODO KKr - use xml parser here
            String key = t.substring(t.indexOf("<Name>") + 6, t.indexOf("</Name>"));
            String val = t.substring(t.indexOf("<Value>") + 7, t.indexOf("</Value>"));
            
            if (m.containsKey(key)){
                String[] oldA = m.get(key);
                String[] newA = new String[ oldA.length + 1 ];
                System.arraycopy(oldA, 0, newA, 0, oldA.length); 
                newA[ newA.length - 1 ] = val;
                m.put(key, newA);
            } else {
                m.put(key, new String[]{val});
            }
        }
        
        return m;
    }
    
    public Map<String, String[]> getAttributes(String domainName, String itemName) throws IOException, AWSException, InterruptedException {
        return getAttributes(domainName, itemName, null, false);
    }
    
    public Map<String, String[]> getAttributes(String domainName, String itemName, boolean consistenRead) throws IOException, AWSException, InterruptedException {
        return getAttributes(domainName, itemName, null, consistenRead);
    }
    
    public String[] getAttribute(String domainName, String itemName, String attributeName) throws IOException, AWSException, InterruptedException {
        return getAttribute(domainName, itemName, attributeName, false);
    }

    public String[] getAttribute(String domainName, String itemName, String attributeName, boolean consistentRead) throws IOException, AWSException, InterruptedException {
        Map<String, String[]> attributes = getAttributes(domainName, itemName, attributeName, consistentRead);
        return attributes.get(attributeName);
    }
    

    /*
     * The Select operation returns a set of Attributes for ItemNames that match
     * the select expression. Select is similar to the standard SQL SELECT statement.
     * 
     * The total size of the response cannot exceed 1 MB in total size. Amazon SimpleDB 
     * automatically adjusts the number of items returned per page to enforce this limit. 
     * For example, even if you ask to retrieve 2500 items, but each individual item is 
     * 10 kB in size, the system returns 100 items and an appropriate next token so you 
     * can get the next page of results.
     */
    public List<Map<String, String[]>> select(String selectExpression) throws IOException, AWSException, InterruptedException {
        
        return select(selectExpression, null, false);
    }
    
    public List<Map<String, String[]>> select(String selectExpression, 
                    boolean consistenRead) throws IOException, AWSException, InterruptedException {
        
        return select(selectExpression, null, consistenRead);
    }
    
    public List<Map<String, String[]>> select(String selectExpression, 
                    String nextToken) throws IOException, AWSException, InterruptedException  {
        
        return select(selectExpression, nextToken, false);
    }

    public List<Map<String, String[]>> select(String selectExpression, String nextToken, 
                    boolean consistentRead) throws IOException, AWSException, InterruptedException  {
        
        Map<String, String> uriParams = createStandardParams("Select");
        uriParams.put("SelectExpression", selectExpression);

        if (nextToken != null) {
            uriParams.put("NextToken", nextToken);
        }
        
        if (consistentRead) {
            uriParams.put("ConsistentRead", "true");
        }
        
        uriParams.put("Signature", getSignature(uriParams));
        String resp = doSimpleGet(uriParams);

        List<Map<String, String[]>> resultList = new ArrayList<Map<String, String[]>>();

        List<String> itemList = _xmlParser.getElements(resp, "Item");
        for (int x = 0; x < itemList.size(); x++) {
            String i = itemList.get(x).toString();

            Map<String, String[]> map = new HashMap<String, String[]>();

            List<String> nameId = _xmlParser.getElements(i, "Name");
            map.put("ItemName", new String[]{nameId.get(0).toString()});

            List<String> attributes = _xmlParser.getElements(i, "Attribute");
            for (int xx = 0; xx < attributes.size(); xx++) {
                String t = attributes.get(xx);

                // TODO KKr - use xml parser
                String key = t.substring(t.indexOf("<Name>") + 6, t.indexOf("</Name>"));
                String val = t.substring(t.indexOf("<Value>") + 7, t.indexOf("</Value>"));
                
                if (map.containsKey(key)) {
                    String[] oldA = map.get(key);
                    String[] newA = new String[ oldA.length + 1 ];
                    System.arraycopy(oldA, 0, newA, 0, oldA.length); 
                    newA[ newA.length - 1 ] = val;
                    map.put(key, newA);
                } else {
                    map.put(key, new String[]{val});
                }
            }

            resultList.add(map);
        }

        return resultList;
    }

    
    
    private String doSimpleGet(Map<String, String> uriParams) throws IOException, AWSException, InterruptedException {
        try {
            String response = _httpHandler.get(getUrl(uriParams));
            processResponse(response);
            return response;
        } catch (HttpException e) {
            String errorResponse = e.getResponse();
            String awsErrorCode = getAWSErrorCode(errorResponse);
            String awsMessage = getErrorMsg(errorResponse);
            throw new AWSException(e.getStatusCode(), awsErrorCode, String.format("%s (%s/%d)", awsMessage, awsErrorCode, e.getStatusCode()), e);
        }
    }

    private String doSimplePost(Map<String, String> uriParams) throws IOException, AWSException, InterruptedException {
        try {
            URL url = new URL("https://" + _httpEndPoint);
            String response = _httpHandler.post(url, uriParams);
            processResponse(response);
            return response;
        } catch (HttpException e) {
            String errorResponse = e.getResponse();
            String awsErrorCode = getAWSErrorCode(errorResponse);
            String awsMessage = getErrorMsg(errorResponse);
            throw new AWSException(e.getStatusCode(), awsErrorCode, String.format("%s (%s/%d)", awsMessage, awsErrorCode, e.getStatusCode()), e);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }

    /*
     * Retrieve the standard Response elements
     */
    private void processResponse(String resp){
        lastRequestId = _xmlParser.getElement(resp, "RequestId");
        lastBoxUsage = _xmlParser.getElement(resp, "BoxUsage");
        lastToken = _xmlParser.getElement(resp, "NextToken");
    }
    
    private String getAWSErrorCode(String response) {
        String result = _xmlParser.getElement(response, "Code");
        if (result == null) {
            result = AWSException.NO_AWS_ERROR_CODE;
        }
        
        return result;
    }
    
    private String getErrorMsg(String response) {
        return _xmlParser.getElement(response, "Message");
    }

    /*
     * Creates the standard Map with all the standard params we require
     * for any particular request
     */
    private Map<String, String> createStandardParams(String action){
        Map<String, String> uriParams = new TreeMap<String, String>();
        uriParams.put("AWSAccessKeyId", _awsId);
        uriParams.put("Action", action);
        uriParams.put("SignatureVersion", SIGNATURE_VERSION);
        uriParams.put("SignatureMethod", SIGNATURE_METHOD);
        uriParams.put("Version", API_VERSION);
        uriParams.put("Timestamp", AWSUtils.getTimestampFromLocalTime(new Date()));
        return uriParams;
    }
    

    /*
     * Given the current Params, we create the signature for this particular
     * request
     */
    private String getSignature(Map<String, String> uriParams) {
        return getSignature(true, _httpEndPoint, uriParams);
    }
        
    private String getSignature(boolean bGet, String http, Map<String, String> uriParams) {
        StringBuilder sb = new StringBuilder(512);

        if (bGet)
            sb.append("GET\n");
        else
            sb.append("POST\n");
        
        if (http.startsWith("http")){
            http = http.substring(http.indexOf("//")+2);
            String uri = http.substring(0, http.indexOf("/"));
            sb.append(uri + "\n");
            
            if (bGet)
                sb.append(http.substring(http.indexOf("/")) + "/\n");
            else
                sb.append(http.substring(http.indexOf("/")) + "\n");
        }else{
            sb.append(http);
            sb.append("\n/\n");
        }
        
        List<String> keys = new ArrayList<String>(uriParams.keySet());
        Collections.sort(keys);
        Iterator<String> iter = keys.iterator();
        
        while (iter.hasNext()) {
            String key = iter.next();
            String val = uriParams.get(key);

            sb.append(key);
            sb.append("=");
            sb.append(safeUrlEncoder(val).replace("+", "%20").replace("*", "%2A").replace("%7E","~"));

            if (iter.hasNext()) {
                sb.append("&");
            }
        }

        return Base64.encodeBytes(hmacSha1(sb.toString()));
    }
    
    private String safeUrlEncoder(String url) {
        try {
            return URLEncoder.encode(url, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }
    
    /*
     * Creates the URL from the parameters to Amazon
     */
    private URL getUrl(Map<String, String> uriParams) {
        StringBuilder sb = new StringBuilder(512);

        sb.append("https://");
        sb.append(_httpEndPoint);
        sb.append("/?");
        
        Iterator<String> it = uriParams.keySet().iterator();
        while (it.hasNext()){
            String key = (String)it.next();
            String val = (String)uriParams.get(key);

            sb.append(key);
            sb.append("=");
            sb.append(safeUrlEncoder(val).replace("+", "%20").replace("*", "%2A").replace("%7E","~"));
            
            if (it.hasNext())
                sb.append("&");
        }
        
        try {
            return new URL(sb.toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }
    
    private synchronized byte[] hmacSha1(String str) {
        try {
            if (_mac == null) {
                _mac = Mac.getInstance(SIGNATURE_METHOD);
            }
            
            _mac.init(secret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        return _mac.doFinal(str.getBytes());
    }


}