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
 * Based on cascading.jdbc code released into the public domain by
 * Concurrent, Inc.
 */

package com.bixolabs.simpledb;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * The SimpleDBScheme class is a {@link Scheme} subclass. It is used in conjunction with the {@SimpleDBTap} to
 * allow for the reading and writing of data to and from Amazon's SimpleDB service.
 *
 * @see SimpleDBTap
 */
@SuppressWarnings("serial")
public class SimpleDBScheme extends Scheme {
    private static final Logger LOGGER = Logger.getLogger(SimpleDBScheme.class);

    private Fields _schemeFields;
    private String _itemFieldName;
    private String _query;
    private int _selectLimit;
    
    public SimpleDBScheme(Fields schemeFields, Fields itemField) {
        this(schemeFields, itemField, "");
    }
    
    public SimpleDBScheme(Fields schemeFields, Fields itemField, String query) {
        this(schemeFields, itemField, query, SimpleDBUtils.NO_SELECT_LIMIT);
    }

    public SimpleDBScheme(Fields schemeFields, Fields itemField, String query, int selectLimit) {
        super(schemeFields, schemeFields);
        
        if (schemeFields.size() == 0) {
            throw new IllegalArgumentException("There must be at least one field");
        }
        
        if (itemField.size() != 1) {
            throw new IllegalArgumentException("There can only be one item field, found: " + itemField.print());
        }

        if (!schemeFields.contains(itemField)) {
            throw new IllegalArgumentException("Scheme fields must include the item field");
        }
        
        _schemeFields = schemeFields;
        
        // TODO KKr - is this OK to assume that I'll always be able to use it as a String?
        // TODO KKr - should I get the position of this in the scheme field, and save/use that?
        _itemFieldName = itemField.get(0).toString();
        
        setQuery(query);
        
        _selectLimit = selectLimit;
    }

    public void setQuery(String query) {
        query = query.trim();
        if (query.startsWith("where ")) {
            throw new IllegalArgumentException("Query should not contain the `where ` portion of the expression: " + query);
        }
        
        _query = query;
    }
    
    public String getQuery() {
        return _query;
    }
    
    public void setSelectLimit(int selectLimit) {
        _selectLimit = selectLimit;
    }
    
    public int getSelectLimit() {
        return _selectLimit;
    }
    
    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(SimpleDBOutputFormat.class);
        
        SimpleDBConfiguration sdbConf = new SimpleDBConfiguration(conf);
        sdbConf.setSchemeFields(_schemeFields);
        sdbConf.setItemFieldName(_itemFieldName);
        
        LOGGER.info(String.format("Initializing SimpleDB sink tap - scheme field: %s and item field: %s", _schemeFields, _itemFieldName));
    }

    public void sourceInit(Tap tap, JobConf conf) throws IOException {
        conf.setInputFormat(SimpleDBInputFormat.class);
        
        SimpleDBConfiguration sdbConf = new SimpleDBConfiguration(conf);
        sdbConf.setSchemeFields(_schemeFields);
        sdbConf.setItemFieldName(_itemFieldName);
        
        sdbConf.setQuery(_query);
        sdbConf.setSelectLimit(_selectLimit);
        
        LOGGER.info(String.format("Initializing SimpleDB source tap - scheme field: %s and item field: %s", _schemeFields, _itemFieldName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        String itemValue = tupleEntry.getString(_itemFieldName);
        if ((itemValue == null) || (itemValue.length() == 0)) {
            throw new TapException("Tuple passed to sink does not have a valid (not null, not empty) value for the item field (" + _itemFieldName + ")");
        }
        
        Tuple result = getSinkFields() != null ? tupleEntry.selectTuple(getSinkFields()) : tupleEntry.getTuple();
        outputCollector.collect(NullWritable.get(), result);
    }

    @Override
    public Tuple source(Object key, Object value) {
        return (Tuple)value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((_itemFieldName == null) ? 0 : _itemFieldName.hashCode());
        result = prime * result + ((_query == null) ? 0 : _query.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleDBScheme other = (SimpleDBScheme) obj;
        if (_itemFieldName == null) {
            if (other._itemFieldName != null)
                return false;
        } else if (!_itemFieldName.equals(other._itemFieldName))
            return false;
        if (_query == null) {
            if (other._query != null)
                return false;
        } else if (!_query.equals(other._query))
            return false;
        return true;
    }


}
