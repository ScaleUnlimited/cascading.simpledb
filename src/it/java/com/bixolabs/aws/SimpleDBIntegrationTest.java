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
package com.bixolabs.aws;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleDBIntegrationTest {

    private SimpleDB _sdb;
    
    @Before
    public void setUp() throws IOException {
        _sdb = new SimpleDB(TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), new BackoffHttpHandler());
    }
    
    @After
    public void tearDown() {
        if (_sdb != null) {
            try {
                _sdb.deleteDomain(TestUtils.TEST_DOMAIN_NAME);
            } catch (Exception e) {
                // ignore
            }
        }
    }
    
    @Test
    public void testConnection() throws Exception {
        assertEquals(TestUtils.TEST_DOMAIN_NAME, _sdb.createDomain(TestUtils.TEST_DOMAIN_NAME));

        boolean foundDomain = false;
        List<String> domains = _sdb.listDomains();
        for (String domain : domains) {
            if (domain.equals(TestUtils.TEST_DOMAIN_NAME)) {
                foundDomain = true;
                break;
            }
        }
        
        assertTrue(foundDomain);
        assertEquals(TestUtils.TEST_DOMAIN_NAME, _sdb.deleteDomain(TestUtils.TEST_DOMAIN_NAME));
        
        domains = _sdb.listDomains();
        for (String domain : domains) {
            if (domain.equals(TestUtils.TEST_DOMAIN_NAME)) {
                fail("found domain that was deleted");
            }
        }
    }
    
    @Test
    public void testDomainMetadata() throws Exception {

        _sdb.createDomain(TestUtils.TEST_DOMAIN_NAME);
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("attr1", "item1-attr1-value1");
        attributes.put("attr2", "item1-attr2-value1");
        _sdb.putAttributes(TestUtils.TEST_DOMAIN_NAME, "item1", attributes);

        // Force everything to be updated.
        _sdb.getAttributes(TestUtils.TEST_DOMAIN_NAME, "item1", true);

        Thread.sleep(2000);

        Map<String, String> metadata = _sdb.domainMetaData(TestUtils.TEST_DOMAIN_NAME);
        for (String key : metadata.keySet()) {
            System.out.println(key + ": " + metadata.get(key));
        }
    }

    @Test
    public void testErrorHandling() throws Exception {

        try {
            _sdb.getAttributes(TestUtils.TEST_DOMAIN_NAME, "item1");
            fail("Exception not thrown");
        } catch (AWSException e) {
            assertEquals(AWSException.NO_SUCH_DOMAIN, e.getAWSErrorCode());
        }
    }
    
    @Test
    public void testAttributeReadWrite() throws Exception {
        _sdb.createDomain(TestUtils.TEST_DOMAIN_NAME);

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("attr1", "item1-attr1-value1");

        _sdb.putAttributes(TestUtils.TEST_DOMAIN_NAME, "item1", attributes);

        String[] result = _sdb.getAttribute(TestUtils.TEST_DOMAIN_NAME, "item1", "attr1", true);
        assertEquals(1, result.length);
        assertEquals("item1-attr1-value1", result[0]);
    }
    
    @Test
    public void testLimit() throws Exception {
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        _sdb.createDomain(domainName);
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("status", "LOADED");

        final int numBaseItems = 10;
        for (int i = 0; i < numBaseItems; i++) {
            _sdb.putAttributes(domainName, "item-" + i, attributes);
        }

        while (true) {
            List<Map<String, String[]>> result = _sdb.select("select count(*) from `" + domainName + "`");
            assertEquals(1, result.size());
            String count = result.get(0).get("Count")[0];
            if (count.equals("" + numBaseItems)) {
                break;
            }
        }
        
        List<Map<String, String[]>> result = _sdb.select("select count(*) from `" + domainName + "` limit 1");
        assertEquals(1, result.size());
        String count = result.get(0).get("Count")[0];
        assertEquals("1", count);
    }

    @Test
    public void testSelection() throws Exception {
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        _sdb.createDomain(domainName);

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("status", "LOADED");

        final int numBaseItems = 10;
        for (int i = 0; i < numBaseItems; i++) {
            _sdb.putAttributes(domainName, "item-" + i, attributes);
        }
        
        attributes.put("status", "ERROR");
        _sdb.putAttributes(domainName, "item-special", attributes);
        
        List<Map<String, String[]>> result = _sdb.select("select count(*) from `" + domainName + "` where status = 'ERROR'", true);
        assertEquals(null, _sdb.getLastToken());
        assertEquals(1, result.size());
        String count = result.get(0).get("Count")[0];
        assertEquals("1", count);
    }
    
    @Test
    public void testConditionalPut() throws Exception {
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        _sdb.createDomain(domainName);

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("attr1", "item1-attr1-value1");
        _sdb.putAttributes(TestUtils.TEST_DOMAIN_NAME, "item1", attributes);
        
        String[] result = _sdb.getAttribute(TestUtils.TEST_DOMAIN_NAME, "item1", "attr1", true);
        assertEquals(1, result.length);
        assertEquals("item1-attr1-value1", result[0]);

        attributes.put("attr1", "item1-attr1-value2");
        Set<String> replaceAttrs = new HashSet<String>();
        replaceAttrs.add("attr1");
        
        _sdb.putAttributes(TestUtils.TEST_DOMAIN_NAME, "item1", attributes, replaceAttrs, "attr1", "item1-attr1-value1", true);
        
        result = _sdb.getAttribute(TestUtils.TEST_DOMAIN_NAME, "item1", "attr1", true);
        assertEquals(1, result.length);
        assertEquals("item1-attr1-value2", result[0]);

        // Now try to change to value3, but this time say that the attribute can't exist.
        attributes.put("attr1", "item1-attr1-value3");
        try {
            _sdb.putAttributes(TestUtils.TEST_DOMAIN_NAME, "item1", attributes, replaceAttrs, "attr1", null, false);
            fail("Should have failed");
        } catch (AWSException e) {
            assertEquals("ConditionalCheckFailed", e.getAWSErrorCode());
        }
        
        // Should still have the old value.
        result = _sdb.getAttribute(TestUtils.TEST_DOMAIN_NAME, "item1", "attr1", true);
        assertEquals(1, result.length);
        assertEquals("item1-attr1-value2", result[0]);
    }
    
    @Test
    public void testBackoff() throws Exception {
        final int numThreads = 300;
        final int numRequestsPerThread = 10;
        final int numAttributes = 1;
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        
        _sdb.createDomain(domainName);

        ThreadGroup tg = new ThreadGroup("testBackoff");

        final Map<String, String> attributes = new HashMap<String, String>();
        for (int i = 1; i <= numAttributes; i++) {
            attributes.put("attr-" + i, "attr-" + i + "-value");
        }
        
        final List<Exception> sdbExceptions = Collections.synchronizedList(new ArrayList<Exception>());

        final AtomicBoolean hold = new AtomicBoolean(true);

        final AtomicInteger reqCount = new AtomicInteger();
        
        IHttpHandler httpHandler = new BackoffHttpHandler(numThreads);
        
        for (int i = 0; i < numThreads; i++) {
            final SimpleDB sdb = new SimpleDB(TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), httpHandler);

            final int threadIndex = i;
            Runnable target = new Runnable() {

                @Override
                public void run() {
                    
                    // Wait till we get the go-ahead to hit the server
                    while (hold.get()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }

                    try {
                        for (int ri = 0; ri < numRequestsPerThread; ri++) {
                            int itemId = (threadIndex * numRequestsPerThread) + numRequestsPerThread;
                            sdb.putAttributes(TestUtils.TEST_DOMAIN_NAME, "item-" + itemId, attributes);
                            reqCount.incrementAndGet();

                            if (sdbExceptions.size() > 0) {
                                // Skip processing more requests once we get an error.
                                break;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        sdbExceptions.add(e);
                    }
                }
            };

            Thread t = new Thread(tg, target);
            t.start();
        }

        System.out.println("Starting requests");
        long startTime = System.currentTimeMillis();
        hold.set(false);

        while (!Thread.interrupted() && (tg.activeCount() > 0)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        long deltaTime = System.currentTimeMillis() - startTime;
        int numPutAttributeRequests = reqCount.get();
        System.out.println(String.format("%d PutAttribute requests took %d millisecond", numPutAttributeRequests, deltaTime));
        assertEquals("Exceptions occurred", 0, sdbExceptions.size());
    }
}
