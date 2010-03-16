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
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import cascading.tuple.Tuple;

import com.bixolabs.aws.AWSException;
import com.bixolabs.aws.BackoffHttpHandler;
import com.bixolabs.aws.IHttpHandler;
import com.bixolabs.aws.SimpleDB;

public class SimpleDBInputFormat implements InputFormat<NullWritable, Tuple>, JobConfigurable {
    
    @Override
    public RecordReader<NullWritable, Tuple> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
        return new SimpleDBRecordReader(split, new SimpleDBConfiguration(conf));
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        SimpleDBConfiguration sdbConf = new SimpleDBConfiguration(conf);
        
        String domainName = sdbConf.getDomainName();
        int numShards = sdbConf.getNumShards();
        String query = sdbConf.getQuery();
        int selectLimit = sdbConf.getSelectLimit();
        int remainingLimit = selectLimit;

        IHttpHandler httpHandler = new BackoffHttpHandler(numShards);
        SimpleDB sdb = new SimpleDB(sdbConf.getAccessKeyId(), sdbConf.getSecretAccessKey(), httpHandler);

        // We want one split per shard.
        List<String> shardNames = SimpleDBUtils.getShardNames(domainName, numShards);
        List<SimpleDBInputSplit> splits = new ArrayList<SimpleDBInputSplit>(numShards);

        // FUTURE KKr - parallelize this, by submitting N tasks, one per shard. We'd have a minor issue
        // with wanting to round up the split limit, and then having to constrain down to not exceed the limit.
        for (int i = 0; i < numShards; i++) {
            // Make a select call to get the count of items.
            
            // Silly code to ensure that even for test cases, the combined shard limits will sum
            // to the actual selectLimit, even with integer division rounding errors.
            int shardLimit = SimpleDBUtils.NO_SELECT_LIMIT;
            if (selectLimit != SimpleDBUtils.NO_SELECT_LIMIT) {
                shardLimit = remainingLimit / (numShards - i);
                
                // During testing, for example, we can wind up with 0 items in a split.
                if (shardLimit == 0) {
                    continue;
                }
            }

            String shardName = shardNames.get(i);
            
            try {
                int numItems = SimpleDBUtils.getItemCount(sdb, shardName, query, shardLimit);
                
                // If we actually have any matches in this shard, generate a split.
                if (numItems > 0) {
                    shardLimit = Math.min(shardLimit, numItems);
                    remainingLimit -= shardLimit;
                    splits.add(new SimpleDBInputSplit(numItems, shardLimit, shardName));
                }
            } catch (AWSException e) {
                throw new IOException("Error getting item count from domain " + shardName, e);
            } catch (InterruptedException e) {
                throw new IOException("Interruption while getting item count from domain " + shardName);
            }
        }
        
        return splits.toArray(new SimpleDBInputSplit[splits.size()]);
    }

    @Override
    public void validateInput(JobConf conf) throws IOException {
        // Do nothing - deprecated
    }

    @Override
    public void configure(JobConf conf) {
        // TODO KKr - what should I be doing here?
    }

}
