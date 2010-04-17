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
 */
package com.bixolabs.simpledb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bixolabs.aws.AWSException;
import com.bixolabs.aws.SimpleDB;

public class SimpleDBUtils {

    public static final int NO_SELECT_LIMIT = -1;
    
    // Name of attribute used to store item name hash, for slicing a shard into chunks.
    public static final String ITEM_HASH_ATTR_NAME = SimpleDBUtils.class.getSimpleName() + "-itemHash";
    
    private static final long MIN_HASH = (long)Integer.MIN_VALUE;
    private static final long HASH_RANGE = (long)Integer.MAX_VALUE - MIN_HASH;
    private static final int HASH_DIGITS = Long.toString(HASH_RANGE).length();
    private static final String NEGATIVE_HASH_FORMAT = "0%0" + HASH_DIGITS + "d";
    private static final String POSITIVE_HASH_FORMAT = "1%0" + HASH_DIGITS + "d";
    
    // These two must be kept in sync.
    private static final String DOMAIN_NAME_FORMAT = "%s-%d-of-%d";
    private static final Pattern DOMAIN_NAME_PATTERN = Pattern.compile("(.+)-(\\d+)-of-(\\d+)");
    
    public static List<String> getShardNames(String baseDomainName, int numShards) {
        List<String> result = new ArrayList<String>(numShards);
        for (int i = 1; i <= numShards; i++) {
            result.add(String.format(DOMAIN_NAME_FORMAT, baseDomainName, i, numShards));
        }
        
        return result;
    }

    /**
     * Given a domain name, return the total shard count (n from x-of-n pattern)
     * 
     * @param domain domain name
     * @return total shards, or -1 if domain name format isn't valid.
     */
    public static int getShardCount(String domain) {
        Matcher m = DOMAIN_NAME_PATTERN.matcher(domain);
        if (!m.matches()) {
            return -1;
        } else {
            return Integer.parseInt(m.group(3));
        }
    }

    /**
     * Given a domain name, return the shard number (1..n)
     * 
     * @param domain domain name
     * @return shard number, or -1 if domain name format isn't valid.
     */
    public static int getShardNumber(String domain) {
        Matcher m = DOMAIN_NAME_PATTERN.matcher(domain);
        if (!m.matches()) {
            return -1;
        } else {
            return Integer.parseInt(m.group(2));
        }
    }

    public static int getNumShardsForTable(SimpleDB sdb, String table) throws IOException, AWSException, InterruptedException {
        int numShards = 0;
        boolean[] shardFound = null;
        
        List<String> domains = sdb.listDomains();
        for (String domain : domains) {
            if (domain.startsWith(table)) {
                int totalShards = getShardCount(domain);
                int shardNumber = getShardNumber(domain);
                
                if ((totalShards != -1) && (shardNumber != -1)) {
                    if (numShards == 0) {
                        numShards = totalShards;
                        shardFound = new boolean[numShards];
                    } else if (numShards != totalShards) {
                        throw new IllegalStateException(String.format("Table %s has shard %s with a different total count than a previous shard", table, domain));
                    }
                    
                    shardFound[shardNumber - 1] = true;
                }
            }
        }
        
        for (int i = 0; i < numShards; i++) {
            if (!shardFound[i]) {
                throw new IllegalStateException(String.format("Table %s is missing shard #%d", table, i+1));
            }
        }

        return numShards;
    }
    
    public static int getShardIndex(String itemValue, int numShards) {
        if (numShards == 1) {
            return 0;
        }

        long shardRange = HASH_RANGE/numShards;
        long hash = joaat_hash(itemValue);
        long absoluteHash = hash - MIN_HASH;
        return (int)(absoluteHash/shardRange);
    }
    
    public static String getItemHash(String itemName) {
        int hash = joaat_hash(itemName);
        
        if (hash < 0) {
            return String.format(NEGATIVE_HASH_FORMAT, Math.abs(hash));
        } else {
            return String.format(POSITIVE_HASH_FORMAT, hash);
        }
    }

    /**
     * Return the total count for items in <domainName>, optionally selected with
     * <expression> and limited to <limit> items.
     * 
     * @param sdb
     * @param domainName
     * @param expression expression to use with selection, or ""/null.
     * @param limit limit to result, or NO_SELECT_LIMIT
     * @return count of total items matching selection criteria
     * @throws IOException
     * @throws AWSException
     * @throws InterruptedException
     */
    public static int getItemCount(SimpleDB sdb, String domainName, String expression,
                    int limit) throws IOException, AWSException, InterruptedException {

        int result = 0;
        String nextToken = null;
        
        // FUTURE KKr - if there's no query, then just make a metadata call as that's
        // going to be faster than the select.
        
        String selectStr = String.format("select count(*) from `%s`", domainName);
        if ((expression != null) && (expression.length() > 0)) {
            selectStr += String.format(" where %s", expression);
        }

        boolean limited = limit != NO_SELECT_LIMIT;
        if (limited) {
            selectStr += String.format(" limit %d", limit);
        }

        // TODO KKr - seems like the select call will keep returning results past what
        // we specified with the limit parameter...verify that this is the expected behavior.
        // I think the fix is to reduce the limit by the returned count, each time through
        // the loop.
        do {
            List<Map<String, String[]>> selectResult = sdb.select(selectStr, nextToken);
            nextToken = sdb.getLastToken();

            String numItemsStr = null;
            for (Map<String, String[]> attributes : selectResult) {
                String[] values = attributes.get("Count");
                if ((values != null) && (values.length > 0)) {
                    numItemsStr = values[0];
                    break;
                }
            }

            if (numItemsStr == null) {
                throw new RuntimeException("SimpleDB select didn't return count");
            }

            try {
                result += Integer.parseInt(numItemsStr);
            } catch (NumberFormatException e) {
                throw new RuntimeException("SimpleDB select returned invalid count: " + numItemsStr);
            }
        } while ((nextToken != null) && (!limited || (result < limit)));

        if (limited) {
            result = Math.min(result, limit);
        }
        
        return result;
    }
    
    
    private static int joaat_hash(String key) {
        try {
            return joaat_hash(key.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible error", e);
        }
    }

    private static int joaat_hash(byte[] key) {
        int hash = 0;
        
        for (byte b : key) {
            hash += (b & 0xFF);
            hash += (hash << 10);
            hash ^= (hash >>> 6);
        }
        
        hash += (hash << 3);
        hash ^= (hash >>> 11);
        hash += (hash << 15);
        
        return hash;
    }


}
