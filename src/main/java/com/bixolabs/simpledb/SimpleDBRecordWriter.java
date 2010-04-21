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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.bixolabs.aws.BackoffHttpHandler;
import com.bixolabs.aws.IHttpHandler;
import com.bixolabs.aws.SimpleDB;

public class SimpleDBRecordWriter implements RecordWriter<NullWritable, Tuple> {
    private static final Logger LOGGER = Logger.getLogger(SimpleDBRecordWriter.class);
    
    
    // This value must be less than the Hadoop job timeout value, as otherwise the
    // job could get killed while waiting for a request to get handled.
    private static final long REJECTED_EXECUTION_TIMEOUT = 300 * 1000L;
    private static final long TERMINATION_TIMEOUT = REJECTED_EXECUTION_TIMEOUT;

    private static final int BATCH_WRITE_SIZE = 25;

    private class SdbShardWriter {

        private class AsyncSdbWriter implements Runnable {
            private Map<String, Map<String, String>> _items;

            public AsyncSdbWriter(Map<String, Map<String, String>> items) {
                _items = items;
            }

            @Override
            public void run() {
                try {
                    // FUTURE KKr - we could skip replacing the item hashvalue attribute, as that
                    // shouldn't ever change for a given item.
                    Map<String, Set<String>> replaceAttr = new HashMap<String, Set<String>>();
                    for (String itemName : _items.keySet()) {
                        replaceAttr.put(itemName, _items.get(itemName).keySet());
                    }
                    
                    long startTime = System.currentTimeMillis();
                    LOGGER.trace(String.format("Updating %s with %d items", _shardName, _items.size()));
                    _sdb.batchPutAttributes(_shardName, _items, replaceAttr);
                    LOGGER.trace(String.format("Updated %s with %d items in %dms", _shardName, _items.size(), System.currentTimeMillis() - startTime));
                } catch (Exception e) {
                    LOGGER.error("Error while putting attributes to SimpleDB", e);
                    
                    IOException ioe;
                    if (e instanceof IOException) {
                        ioe = new IOException("Error while putting attributes to SimpleDB");
                        ioe.setStackTrace(e.getStackTrace());
                    } else {
                        ioe = new IOException("Error while putting attributes to SimpleDB", e);
                    }
                    
                    _exceptions.add(ioe);
                }
            }
        }
        
        private final String _shardName;
        private final SimpleDB _sdb;
        
        private Map<String, Map<String, String>> _queue;

        public SdbShardWriter(SimpleDB sdb, String shardName) {
            _shardName = shardName;
            _sdb = sdb;
            
            _queue = new LinkedHashMap<String, Map<String, String>>();
        }
        
        public void put(String itemName, Map<String, String> attributes) throws IOException {
            _queue.put(itemName, attributes);
            if (_queue.size() >= BATCH_WRITE_SIZE) {
                writeQueue();
            }
        }
        
        public void writeQueue() throws IOException {
            try {
                if (_queue.size() > 0) {
                    Map<String, Map<String, String>> curQueue = new LinkedHashMap<String, Map<String, String>>(_queue);

                    LOGGER.trace(String.format("Queuing up %d items for %s", curQueue.size(), _shardName));

                    _executor.execute(new AsyncSdbWriter(curQueue));
                    _queue.clear();
                }
            } catch (RejectedExecutionException e) {
                String msg = "Async write to SimpleDB rejected";
                LOGGER.error(msg);
                throw new IOException(msg, e);
            }
        }
    }

    private String _domainName;
    private int _numShards;
    private Fields _schemeFields;
    private String _itemFieldName;
    private List<IOException> _exceptions;
    
    private SdbShardWriter[] _shardWriters;
    private ThreadedExecutor _executor;

    public SimpleDBRecordWriter(SimpleDBConfiguration sdbConf) {
        _domainName = sdbConf.getDomainName();
        _numShards = sdbConf.getNumShards();
        _schemeFields = sdbConf.getSchemeFields();
        _itemFieldName = sdbConf.getItemFieldName();

        List<String> shardNames = SimpleDBUtils.getShardNames(_domainName, _numShards);
        _shardWriters = new SdbShardWriter[_numShards];
        
        // One handler gets shared across all shards, but it's multi-threaded
        IHttpHandler httpHandler = new BackoffHttpHandler(sdbConf.getMaxThreads());
        _executor = new ThreadedExecutor(sdbConf.getMaxThreads(), REJECTED_EXECUTION_TIMEOUT);

        LOGGER.trace(String.format("Creating shard writers for %d shards of table %s", _numShards, _domainName));
        
        for (int i = 0; i < _numShards; i++) {
            SimpleDB sdb = new SimpleDB(sdbConf.getAccessKeyId(), sdbConf.getSecretAccessKey(), httpHandler);
            _shardWriters[i] = new SdbShardWriter(sdb, shardNames.get(i));
        }
        
        // We also need to be able to record exceptions that happen during the async writes.
        _exceptions = Collections.synchronizedList(new ArrayList<IOException>());
    }
    
    @Override
    public void write(NullWritable key, Tuple value) throws IOException {
        throwAsyncException();
        
        TupleEntry entry = new TupleEntry(_schemeFields, value);

        String itemName = null;
        Map<String, String> attributes = new HashMap<String, String>();
        for (int i = 0; i < _schemeFields.size(); i++) {
            String fieldName = _schemeFields.get(i).toString();
            String fieldValue = entry.getString(fieldName);

            if (fieldName.equals(_itemFieldName)) {
                itemName = fieldValue;
                
                // Also add the special attribute we use for segmenting a shard (domain)
                attributes.put(SimpleDBUtils.ITEM_HASH_ATTR_NAME, SimpleDBUtils.getItemHash(itemName));
            } else if (fieldValue != null) {
                attributes.put(fieldName, fieldValue);
            }
        }

        int shardIndex = SimpleDBUtils.getShardIndex(itemName, _numShards);
        _shardWriters[shardIndex].put(itemName, attributes);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        for (int i = 0; i < _numShards; i++) {
            _shardWriters[i].writeQueue();
        }
        
        try {
            if (!_executor.terminate(TERMINATION_TIMEOUT)) {
                String msg = "Had to do a hard termination of async writes to SimpleDB";
                LOGGER.warn(msg);
                _exceptions.add(new IOException(msg));
            }
        } catch (InterruptedException e) {
            String msg = "Interrupted while waiting for SimpleDB async writer termination";
            LOGGER.warn(msg);
            _exceptions.add(new IOException(msg));
        }
        
        throwAsyncException();
    }
    
    private void throwAsyncException() throws IOException {
        if (_exceptions.size() > 0) {
            // We're going to pretend that a previous exception actually happened
            // now, with this write.
            IOException ioe = _exceptions.remove(0);
            throw ioe;
        }
    }

}