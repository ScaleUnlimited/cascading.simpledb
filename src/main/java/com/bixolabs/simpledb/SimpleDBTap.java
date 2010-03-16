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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.bixolabs.aws.AWSException;
import com.bixolabs.aws.BackoffHttpHandler;
import com.bixolabs.aws.SimpleDB;

/**
 * The SimpleDB class is a {@link Tap} subclass. It is used in conjunction with the {@SimpleDBScheme}
 * to allow for the reading and writing of data to and from Amazon's SimpleDB service.
 */
@SuppressWarnings("serial")
public class SimpleDBTap extends Tap {
    private static final Logger LOGGER = Logger.getLogger(SimpleDBTap.class);

    public static final String SCHEME = "simpledb";

    private String _accessKeyId;
    private String _secretAccessKey;
    private String _baseDomainName;
    private int _numShards;

    private transient SimpleDB _sdb;

    public SimpleDBTap(SimpleDBScheme scheme, String accessKeyId, String secretAccessKey, String baseDomainName, int numShards) {
        this(scheme, accessKeyId, secretAccessKey, baseDomainName, numShards, SinkMode.UPDATE);
    }

    public SimpleDBTap(SimpleDBScheme scheme, String accessKeyId, String secretAccessKey, String baseDomainName, int numShards, SinkMode sinkMode) {
        super(scheme, sinkMode);
        
        _accessKeyId = accessKeyId;
        _secretAccessKey = secretAccessKey;
        _baseDomainName = baseDomainName;
        _numShards = numShards;
    }

    public Path getPath() {
        return new Path(getURI().toString());
    }

    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
        return new TupleEntryIterator(getSourceFields(), new TapIterator(this, conf));
    }

    public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
        return new TapCollector(this, conf);
    }

    public boolean makeDirs(JobConf conf) throws IOException {

        boolean cleanup = true;
        
        try {
            // FUTURE KKr - multi-thread this code.
            List<String> shardNames = getShardNames();
            for (String shardName : shardNames) {
                getSimpleDB().createDomain(shardName);
            }
            
            // We created all of them, so we're all set.
            cleanup = false;
        } catch (AWSException e) {
            throw new IOException("Error creating domain(s)", e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while creating domain(s)");
        } finally {
            // TODO KKr - when does makeDirs get called? Do I need to delete
            // all of the tables.
            // delete these tables?
            if (cleanup) {
                try {
                    // deletePath(conf);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        
        return true;
    }

    public boolean deletePath(JobConf conf) throws IOException {
        
        try {
            // FUTURE KKr - multi-thread this code.
            List<String> domainNames = getSimpleDB().listDomains();
            for (String domainName : domainNames) {
                if (domainName.startsWith(_baseDomainName)) {
                    getSimpleDB().deleteDomain(domainName);
                }
            }
        } catch (AWSException e) {
            throw new IOException("Error deleting domain(s)", e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while deleting domain(s)");
        }
        
        return true;
    }

    public boolean pathExists(JobConf conf) throws IOException {
        List<String> existingDomains;
        
        try {
            existingDomains = getSimpleDB().listDomains();
        } catch (AWSException e) {
            throw new IOException("Error listing domains", e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while listing domains");
        }
        
        Set<String> domainSet = new HashSet<String>(existingDomains);
        
        List<String> shardNames = getShardNames();
        for (String shardName : shardNames) {
            if (!domainSet.contains(shardName)) {
                return false;
            }
        }
        
        return true;
    }

    public long getPathModified(JobConf conf) throws IOException {
        long mostRecentTime = 0;
        
        List<String> shards = getShardNames();
        for (String shard : shards) {
            // FUTURE KKr - multithread this.
            long lastModTime = getLastModified(shard);
            if (lastModTime > mostRecentTime) {
                mostRecentTime = lastModTime;
            }
        }
        
        return mostRecentTime;
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
        LOGGER.debug("sinking to domain: " + _baseDomainName);

        // do not delete if initialized from within a task
        if (isReplace() && (conf.get("mapred.task.partition") == null)) {
            deletePath(conf);
        }
        
        makeDirs(conf);
        setConf(conf);

        super.sinkInit(conf);
    }

    @Override
    public void sourceInit(JobConf conf) throws IOException {
        LOGGER.debug("sourcing from domain: " + _baseDomainName);

        FileInputFormat.setInputPaths(conf, _baseDomainName);
        
        setConf(conf);
        
        super.sourceInit(conf);
    }
    
    private SimpleDB getSimpleDB() {
        if (_sdb == null) {
            _sdb = new SimpleDB(_accessKeyId, _secretAccessKey, new BackoffHttpHandler(_numShards));
        }
        
        return _sdb;
    }

    private void setConf(JobConf conf) {
        SimpleDBConfiguration sdbConf = new SimpleDBConfiguration(conf);
        sdbConf.setAccessKeyId(_accessKeyId);
        sdbConf.setSecretAccessKey(_secretAccessKey);
        sdbConf.setDomainName(_baseDomainName);
        sdbConf.setNumShards(_numShards);
    }

    private URI getURI() {
        try {
            return new URI(SCHEME, "//" + _accessKeyId + "/" + _baseDomainName, null);
        } catch (URISyntaxException exception) {
            throw new TapException("unable to create uri", exception);
        }
    }
    
    private List<String> getShardNames() {
        return SimpleDBUtils.getShardNames(_baseDomainName, _numShards);
    }
    
    private long getLastModified(String domainName) throws IOException {
        Map<String, String> metadata;
        try {
            metadata = getSimpleDB().domainMetaData(domainName);
        } catch (AWSException e) {
            throw new IOException("Exception getting domain metadata", e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while getting domain metadata");
        }
        
        String timestamp = metadata.get(SimpleDB.TIMESTAMP_METADATA);
        if (timestamp != null) {
            try {
                return Long.parseLong(timestamp);
            } catch (NumberFormatException e) {
                LOGGER.error("SimpleDB metadata returned invalid timestamp for " + _baseDomainName + ": " + timestamp);
            }
        } else {
            LOGGER.error("SimpleDB metadata doesn't contain timestamp " + _baseDomainName);
        }

        return System.currentTimeMillis();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((_accessKeyId == null) ? 0 : _accessKeyId.hashCode());
        result = prime * result + ((_baseDomainName == null) ? 0 : _baseDomainName.hashCode());
        result = prime * result + ((_secretAccessKey == null) ? 0 : _secretAccessKey.hashCode());
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
        SimpleDBTap other = (SimpleDBTap) obj;
        if (_accessKeyId == null) {
            if (other._accessKeyId != null)
                return false;
        } else if (!_accessKeyId.equals(other._accessKeyId))
            return false;
        if (_baseDomainName == null) {
            if (other._baseDomainName != null)
                return false;
        } else if (!_baseDomainName.equals(other._baseDomainName))
            return false;
        if (_secretAccessKey == null) {
            if (other._secretAccessKey != null)
                return false;
        } else if (!_secretAccessKey.equals(other._secretAccessKey))
            return false;
        return true;
    }
    

}
