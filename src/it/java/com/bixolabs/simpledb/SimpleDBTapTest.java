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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import com.bixolabs.aws.SimpleDB;
import com.bixolabs.aws.TestUtils;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;


public class SimpleDBTapTest {

    @After
    public void tearDown() {
        try {
            SimpleDB sdb = new SimpleDB(TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey());
            List<String> domains = sdb.listDomains();
            for (String domain : domains) {
                if (domain.startsWith(TestUtils.TEST_DOMAIN_NAME)) {
                    try {
                        sdb.deleteDomain(domain);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void testSchemeChecks() {
        try {
            new SimpleDBScheme(new Fields(), new Fields("a"));
            fail("Exception should be thrown when scheme field is empty");
        } catch (Exception e) {
        }

        try {
            new SimpleDBScheme(new Fields("a", "b", "c"), new Fields("a", "d"));
            fail("Exception should be thrown when item field isn't exactly one item");
        } catch (Exception e) {
        }

        try {
            new SimpleDBScheme(new Fields("a", "b", "c"), new Fields("d"));
            fail("Exception should be thrown when item field isn't in scheme fields");
        } catch (Exception e) {
        }

    }

    @Test
    public void testWritingTuples() throws Exception {
        String in = "build/test/SimpleDBTapTest/testWritingTuples/in";

        final int numShards = 4;
        final int numItems = 10;
        
        // There's one hidden attribute (the item hash) for each item.
        // So we have have two attributes we write out ("rank", "value")
        // plus the item hash attribute.
        final int numAttributes = numItems * (2 + 1);
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields testFields = new Fields("key", "rank", "value", "extra");
        
        Lfs lfsSource = makeSourceTuples(in, testFields, numItems);

        Pipe pipe = new Pipe("test");
        
        // Skip the "extra" field when writing out tuples, to further complicate things
        Fields schemeFields = new Fields("key", "rank", "value");
        SimpleDBScheme scheme = new SimpleDBScheme(schemeFields, new Fields("key"));
        Tap sink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        
        Flow flow = new FlowConnector().connect(lfsSource, sink, pipe);
        flow.complete();
        
        SimpleDB sdb = new SimpleDB(TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey());
        List<String> shardNames = SimpleDBUtils.getShardNames(domainName, numShards);
        
        int actualItems = 0;
        int atualAttributeValues = 0;
        int actualAttributeValuesSize = 0;
        
        for (String shardName : shardNames) {
            Map<String, String> metadata = sdb.domainMetaData(shardName);
            actualItems += Integer.parseInt(metadata.get("ItemCount"));
            atualAttributeValues += Integer.parseInt(metadata.get("AttributeValueCount"));
            actualAttributeValuesSize += Integer.parseInt(metadata.get("AttributeValuesSizeBytes"));
        }
        
        assertEquals(numItems, actualItems);
        assertEquals(numAttributes, atualAttributeValues);
        
        // 10 ranks and 10 values and 10 item hashes
        int attributeBytes = (numItems * "rank-0".length()) + (numItems * "value-0".length())
            + (numItems * 11);
        assertEquals(attributeBytes, actualAttributeValuesSize);
    }
    
    @Test
    public void testBatchWriting() throws Exception {
        final int numItems = 25;
        final int numShards = 1;
        final String domainName  = TestUtils.TEST_DOMAIN_NAME;
        final Fields itemField = new Fields("key");
        final Fields attrFields = new Fields("value");
        final Fields testFields = itemField.append(attrFields);
        
        String in = "build/test/SimpleDBTapTest/testRoundTripTypeConversion/in";

        Lfs lfsSource = makeSourceTuples(in, testFields, numItems);
        
        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, itemField);
        Tap sdbSink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);

        Flow flow = new FlowConnector().connect(lfsSource, sdbSink, pipe);
        flow.complete();

        // Now verify how much was written out.
        SimpleDB sdb = new SimpleDB(TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey());
        Map<String, String> metadata = sdb.domainMetaData(SimpleDBUtils.getShardNames(domainName, 1).get(0));
        assertEquals("" + numItems, metadata.get("ItemCount"));
    }
    
    @Test
    public void testUpdatingItem() throws Exception {
        final int numShards = 1;
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields testFields = new Fields("key", "value");

        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, new Fields("key"));
        Tap sink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);

        // Do an initial write
        String in = "build/test/SimpleDBTapTest/testUpdatingItem/in";
        Lfs lfs = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        write.add(new Tuple("key-0", "value-0"));
        write.close();
        Flow flow = new FlowConnector().connect(lfs, sink, pipe);
        flow.complete();

        // Update that same entry with a new value
        write = lfs.openForWrite(new JobConf());
        write.add(new Tuple("key-0", "value-1"));
        write.close();
        flow = new FlowConnector().connect(lfs, sink, pipe);
        flow.complete();

        // Read in that item, and verify the value.
        String out = "build/test/SimpleDBTapTest/testUpdatingItem/out";
        Tap sdbSource = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Tap lfsSink = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        flow = new FlowConnector().connect(sdbSource, lfsSink, pipe);
        flow.complete();

        // Make sure both values are in the table now
        TupleEntryIterator sinkIter = lfsSink.openForRead(new JobConf());
        assertTrue(sinkIter.hasNext());
        TupleEntry entry = sinkIter.next();
        assertEquals("value-1", entry.getString("value"));
        assertFalse(sinkIter.hasNext());
    }
    
    @Test
    public void testWritingMultipleTimes() throws Exception {
        final int numShards = 1;
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields testFields = new Fields("key", "value");

        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, new Fields("key"));
        Tap sink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);

        // Do an initial write
        String in = "build/test/SimpleDBTapTest/testWritingMultipleTimes/in1";
        Lfs lfs = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        write.add(new Tuple("key-0", "value-0"));
        write.close();
        Flow flow = new FlowConnector().connect(lfs, sink, pipe);
        flow.complete();
        
        // Now do a second write
        in = "build/test/SimpleDBTapTest/testWritingMultipleTimes/in2";
        lfs = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        write = lfs.openForWrite(new JobConf());
        write.add(new Tuple("key-1", "value-1"));
        write.close();
        flow = new FlowConnector().connect(lfs, sink, pipe);
        flow.complete();
        
        final int numItems = 2;
        
        // Make sure both values are in the table now
        SimpleDB sdb = new SimpleDB(TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey());
        List<String> shardNames = SimpleDBUtils.getShardNames(domainName, numShards);
        assertEquals(1, shardNames.size());
        String shardName = shardNames.get(0);
        Map<String, String> metadata = sdb.domainMetaData(shardName);
        assertEquals("" + numItems, metadata.get("ItemCount"));
        
        // We have two attributes per each item, the one we explicitly write
        // out ("value") and the implicit item hash
        assertEquals("" + (numItems * 2), metadata.get("AttributeValueCount"));
        
        // 2 values of 7 bytes ("value-X") each, plus 2 values of 11 bytes (0-padded hash)
        assertEquals("" + 36, metadata.get("AttributeValuesSizeBytes"));
        
        for (int i = 0; i < 2; i++) {
            String[] values = sdb.getAttribute(shardName, "key-" + i, "value");
            assertEquals(1, values.length);
            assertEquals("value-" + i, values[0]);
        }
    }
    
    @Test
    public void testWriteThenRead() throws Exception {
        final int numShards = 1;
        final int numRecords = 100;
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields testFields = new Fields("key", "value");
        final String in = "build/test/SimpleDBTapTest/testWriteThenRead/in";
        final String out = "build/test/SimpleDBTapTest/testWriteThenRead/out";
        
        Lfs lfsSource = makeSourceTuples(in, testFields, numRecords);
        
        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, new Fields("key"));
        Tap sdbSink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Flow flow = new FlowConnector().connect(lfsSource, sdbSink, pipe);
        flow.complete();

        // Now read back in the values.
        Tap sdbSource = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Tap lfsSink = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        flow = new FlowConnector().connect(sdbSource, lfsSink, pipe);
        flow.complete();
        
        // Now verify that what we read in matches what we wrote out.
        TupleEntryIterator sourceIter = lfsSource.openForRead(new JobConf());
        List<Tuple> sourceTuples = new ArrayList<Tuple>(numRecords);
        while (sourceIter.hasNext()) {            
            sourceTuples.add(new Tuple(sourceIter.next().getTuple()));
        }
        
        TupleEntryIterator sinkIter = lfsSink.openForRead(new JobConf());
        List<Tuple> sinkTuples = new ArrayList<Tuple>(numRecords);
        while (sinkIter.hasNext()) {            
            sinkTuples.add(new Tuple(sinkIter.next().getTuple()));
        }

        assertEquals(numRecords, sourceTuples.size());
        assertEquals(sourceTuples.size(), sinkTuples.size());

        Comparator<Tuple> tupleComparator = new Comparator<Tuple>() {

            @Override
            public int compare(Tuple o1, Tuple o2) {
                return o1.getString(0).compareTo(o2.getString(0));
            }
            
        };
        
        Collections.sort(sourceTuples, tupleComparator);
        Collections.sort(sinkTuples, tupleComparator);
        
        for (int i = 0; i < numRecords; i++) {
            assertEquals(sourceTuples.get(i), sinkTuples.get(i));
        }
    }
    
    @Test
    public void testRoundTripTypeConversion() throws Exception {
        final int numShards = 1;
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields itemField = new Fields("key");
        final Fields attrFields = new Fields("booleanValue", "intValue", "longValue", "doubleValue");
        final Fields testFields = itemField.append(attrFields);
        
        String in = "build/test/SimpleDBTapTest/testRoundTripTypeConversion/in";
        String out = "build/test/SimpleDBTapTest/testRoundTripTypeConversion/out";

        Lfs lfs = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        
        write.add(new Tuple("key", true, 100, Long.MAX_VALUE, 1.0));
        write.close();
        
        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, itemField);
        Tap sdbSink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);

        Flow flow = new FlowConnector().connect(lfs, sdbSink, pipe);
        flow.complete();

        // Now read back in the values.
        Tap sdbSource = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Tap lfsSink = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        
        flow = new FlowConnector().connect(sdbSource, lfsSink, pipe);
        flow.complete();

        TupleEntryIterator sinkTuples = lfsSink.openForRead(new JobConf());
        assertTrue(sinkTuples.hasNext());
        TupleEntry t = sinkTuples.next();
        assertEquals(true, t.getBoolean("booleanValue"));
        assertEquals(100, t.getInteger("intValue"));
        assertEquals(Long.MAX_VALUE, t.getLong("longValue"));
        assertEquals(1.0, t.getDouble("doubleValue"), .00001);
    }
    
    @Test
    public void testSelectLimit() throws Exception {
        final int numShards = 5;
        final int numRecords = 10;
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields testFields = new Fields("key", "value");
        final String in = "build/test/SimpleDBTapTest/testQuery/in";
        final String out = "build/test/SimpleDBTapTest/testQuery/out";
        
        Lfs lfsSource = makeSourceTuples(in, testFields, numRecords);
        
        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, new Fields("key"));
        Tap sdbSink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Flow flow = new FlowConnector().connect(lfsSource, sdbSink, pipe);
        flow.complete();

        // Now read back in the values, with a limit
        scheme.setSelectLimit(2);
        Tap sdbSource = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Tap lfsSink = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        flow = new FlowConnector().connect(sdbSource, lfsSink, pipe);
        flow.complete();

        // Make sure we got back what we expected.
        TupleEntryIterator sinkTuples = lfsSink.openForRead(new JobConf());
        assertTrue(sinkTuples.hasNext());
        sinkTuples.next();
        assertTrue(sinkTuples.hasNext());
        sinkTuples.next();
        assertFalse(sinkTuples.hasNext());
    }
    
    @Test
    public void testQuery() throws Exception {
        final int numShards = 1;
        final int numRecords = 11; // 0...10
        final String domainName = TestUtils.TEST_DOMAIN_NAME;
        final Fields testFields = new Fields("key", "value");
        final String in = "build/test/SimpleDBTapTest/testQuery/in";
        final String out = "build/test/SimpleDBTapTest/testQuery/out";
        
        Lfs lfsSource = makeSourceTuples(in, testFields, numRecords);
        
        Pipe pipe = new Pipe("test");
        SimpleDBScheme scheme = new SimpleDBScheme(testFields, new Fields("key"));
        Tap sdbSink = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Flow flow = new FlowConnector().connect(lfsSource, sdbSink, pipe);
        flow.complete();

        // Now read back in the values, using a query that only selects key-1 and key-11
        scheme.setQuery("itemName() like 'key-1%'");
        Tap sdbSource = new SimpleDBTap(scheme, TestUtils.getAccessKeyID(), TestUtils.getSecretAccessKey(), domainName, numShards);
        Tap lfsSink = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        flow = new FlowConnector().connect(sdbSource, lfsSink, pipe);
        flow.complete();

        // Make sure we got back what we expected.
        TupleEntryIterator sinkTuples = lfsSink.openForRead(new JobConf());
        assertTrue(sinkTuples.hasNext());
        TupleEntry t = sinkTuples.next();
        assertTrue(t.getString("key").startsWith("key-1"));
        assertTrue(sinkTuples.hasNext());
        t = sinkTuples.next();
        assertTrue(t.getString("key").startsWith("key-1"));
        assertFalse(sinkTuples.hasNext());
    }
    
    private Lfs makeSourceTuples(String path, Fields fields, int numTuples) throws IOException {
        Lfs lfs = new Lfs(new SequenceFile(fields), path, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        
        for (int i = 0; i < numTuples; i++) {
            Tuple t = new Tuple();
            for (int j = 0; j < fields.size(); j++) {
                String value = fields.get(j).toString() + "-" + i;
                t.add(value);
            }
            
            write.add(t);
        }
        
        write.close();
        return lfs;
    }
    
}
