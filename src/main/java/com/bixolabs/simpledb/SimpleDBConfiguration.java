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

import org.apache.hadoop.mapred.JobConf;

import cascading.tuple.Fields;
import cascading.util.Util;

public class SimpleDBConfiguration {
        
    public static final int DEFAULT_MAX_THREADS = 100;

    private static final String DOMAIN_NAME_PROPERTY = makePropertyName("domainName");
    private static final String NUM_SHARDS_PROPERTY = makePropertyName("numShards");
    private static final String SCHEME_FIELDS_PROPERTY = makePropertyName("schemeFields");
    private static final String ITEM_FIELD_NAME_PROPERTY = makePropertyName("itemFieldName");
    private static final String QUERY_PROPERTY = makePropertyName("query");
    private static final String ACCESS_KEY_ID_PROPERTY = makePropertyName("accessKeyId");
    private static final String SECRET_ACCESS_KEY_PROPERTY = makePropertyName("secretAccessKey");
    private static final String SELECT_LIMIT_PROPERTY = makePropertyName("selectLimit");
    private static final String MAX_THREADS_PROPERTY = makePropertyName("maxThreads");
    
    private JobConf _conf;
    
    public SimpleDBConfiguration(JobConf conf) {
        _conf = conf;
    }
    
    public void setNumShards(int numShards) {
        _conf.setInt(NUM_SHARDS_PROPERTY, numShards);
    }

    public int getNumShards() {
        return _conf.getInt(NUM_SHARDS_PROPERTY, 1);
    }
    
    public void setDomainName(String domainName) {
        _conf.set(DOMAIN_NAME_PROPERTY, domainName);
    }
    
    public String getDomainName() {
        return _conf.get(DOMAIN_NAME_PROPERTY);
    }

    public void setSchemeFields(Fields fields) {
        _conf.set(SCHEME_FIELDS_PROPERTY, safeSerializeBase64(fields));
    }
    
    public Fields getSchemeFields() {
        return (Fields)safeDeserializeBase64(_conf.get(SCHEME_FIELDS_PROPERTY));
    }
    
    public void setItemFieldName(String itemFieldName) {
        _conf.set(ITEM_FIELD_NAME_PROPERTY, itemFieldName);
    }
    
    public String getItemFieldName() {
        return _conf.get(ITEM_FIELD_NAME_PROPERTY);
    }
    
    public void setQuery(String query) {
        _conf.set(QUERY_PROPERTY, query);
    }

    public String getQuery() {
        return _conf.get(QUERY_PROPERTY);
    }
    
    public void setAccessKeyId(String accessKeyId) {
        _conf.set(ACCESS_KEY_ID_PROPERTY, accessKeyId);
    }

    public String getAccessKeyId() {
        return _conf.get(ACCESS_KEY_ID_PROPERTY);
    }

    public void setSecretAccessKey(String secretAccessKey) {
        _conf.set(SECRET_ACCESS_KEY_PROPERTY, secretAccessKey);
    }

    public String getSecretAccessKey() {
        return _conf.get(SECRET_ACCESS_KEY_PROPERTY);
    }

    public void setSelectLimit(int selectLimit) {
        _conf.setInt(SELECT_LIMIT_PROPERTY, selectLimit);
    }
    
    public int getSelectLimit() {
        return _conf.getInt(SELECT_LIMIT_PROPERTY, SimpleDBUtils.NO_SELECT_LIMIT);
    }
    
    public void setMaxThreads(int maxThreads) {
        _conf.setInt(MAX_THREADS_PROPERTY, maxThreads);
    }
    
    public int getMaxThreads() {
        return _conf.getInt(MAX_THREADS_PROPERTY, DEFAULT_MAX_THREADS);
    }
    
    private static String safeSerializeBase64(Object o) {
        try {
            return Util.serializeBase64(o);
        } catch (IOException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }
    
    private static Object safeDeserializeBase64(String s) {
        try {
            return Util.deserializeBase64(s);
        } catch (IOException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }
    
    private static String makePropertyName(String baseName) {
        return String.format("%s-%s", SimpleDBConfiguration.class.getSimpleName(), baseName);
    }

}
