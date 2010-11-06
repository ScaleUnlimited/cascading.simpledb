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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.bixolabs.aws.BackoffHttpHandler;
import com.bixolabs.aws.IHttpHandler;
import com.bixolabs.aws.SimpleDB;

public class SimpleDBRecordReader implements RecordReader<NullWritable, Tuple> {
    private static final Logger LOGGER = Logger.getLogger(SimpleDBRecordReader.class);

    private String _shardName;
    private Fields _schemeFields;
    private String _itemFieldName;
    private String _query;
    private int _selectLimit;
    
    private SimpleDB _sdb;
    private long _pos;
    private long _length;
    private String _nextToken;
    private List<Map<String, String[]>> _curItems;
    private int _curItemIndex;
    
    public SimpleDBRecordReader(InputSplit split, SimpleDBConfiguration sdbConf) throws IOException {
        SimpleDBInputSplit sdbSplit = (SimpleDBInputSplit)split;
        
        // FUTURE KKr - if we've got more than a threshold number of items (split.getLength()), then
        // we want to parallelize here by sub-selecting with the item hash
        _shardName = sdbSplit.getLocations()[0];
        _schemeFields = sdbConf.getSchemeFields();
        _itemFieldName = sdbConf.getItemFieldName();
        _query = sdbConf.getQuery();
        _selectLimit = sdbSplit.getSelectLimit();
        
        IHttpHandler httpHandler = new BackoffHttpHandler(sdbConf.getMaxThreads());
        _sdb = new SimpleDB(sdbConf.getSdbHost(), sdbConf.getAccessKeyId(), sdbConf.getSecretAccessKey(), httpHandler);
        _nextToken = null;
        _curItems = null;
        
        _pos = 0;
        _length = split.getLength();
    }
    
    @Override
    public void close() throws IOException {
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public Tuple createValue() {
        // TODO KKr - this feels wrong
        return new Tuple(new Object[_schemeFields.size()]);
    }

    @Override
    public long getPos() throws IOException {
        return _pos;
    }

    @Override
    public float getProgress() throws IOException {
        return (float)_pos/(float)_length;
    }

    @Override
    public boolean next(NullWritable key, Tuple value) throws IOException {
        if ((_curItems == null) || (_curItemIndex >= _curItems.size())) {
            
            // Short-circuit for case where there will be no more items.
            if ((_curItems != null) && (_nextToken == null)) {
                _curItems = null;
                return false;
            }
            
            try {
                String selectStr = String.format("select * from `%s`", _shardName);
                if (_query.length() > 0) {
                    selectStr += String.format(" where %s", _query);
                }
                
                if (_selectLimit != SimpleDBUtils.NO_SELECT_LIMIT) {
                    selectStr += String.format(" limit %d", _selectLimit);
                }
                
                LOGGER.trace(String.format("Making select request: %s", selectStr));
                
                _curItems = _sdb.select(selectStr, _nextToken);
                _curItemIndex = 0;
                
                // If we're looping, we need to reduce our limit each time.
                if (_selectLimit != SimpleDBUtils.NO_SELECT_LIMIT) {
                    // Just for safety, trim what we get back to be no more than our limit.
                    if (_curItems.size() > _selectLimit) {
                        _curItems.subList(_selectLimit, _curItems.size()).clear();
                    }
                    
                    _selectLimit -= _curItems.size();
                    if (_selectLimit > 0) {
                        _nextToken = _sdb.getLastToken();
                    } else {
                        _nextToken = null;
                    }
                } else {
                    _nextToken = _sdb.getLastToken();
                }
            } catch (Exception e) {
                throw new IOException("Error selecting from " + _shardName, e);
            }

            if (_curItems.size() == 0) {
                _curItems = null;
                return false;
            }
        }
        
        // FUTURE KKr - return a linked list from sdb.select, and then remove items from
        // from to back as we process them, to save on memory usage.
        
        Map<String, String[]> values = _curItems.get(_curItemIndex++);
        
        // Pick off the actual item name, which is baked into the response by the SimpleDB code.
        String itemValue = values.get("ItemName")[0];
        TupleEntry entry = new TupleEntry(_schemeFields, value);
        entry.set(_itemFieldName, itemValue);
        
        for (int i = 0; i < _schemeFields.size(); i++) {
            String attrName = _schemeFields.get(i).toString();
            String[] attrValues = values.get(attrName);
            if ((attrValues != null) && (attrValues.length > 0)) {
                 entry.set(attrName, attrValues[0]);
                _pos += attrValues[0].length();
            }
        }
        
        return true;
    }
    
}
