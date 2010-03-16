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

import java.util.List;

import static junit.framework.Assert.*;

import org.junit.Test;


public class SimpleDBUtilsTest {

    @Test
    public void testHashValues() {
        String out = SimpleDBUtils.getItemHash("test");
       assertEquals(11, out.length());
    }
    
    @Test
    public void testRoundTripDomainNames() {
        final int numShards = 10;
        List<String> domainNames = SimpleDBUtils.getShardNames("base", numShards);
        for (int i = 1; i <= numShards; i++) {
            String domainName = domainNames.get(i - 1);
            assertEquals(i, SimpleDBUtils.getShardNumber(domainName));
            assertEquals(numShards, SimpleDBUtils.getShardCount(domainName));
        }
    }
}
