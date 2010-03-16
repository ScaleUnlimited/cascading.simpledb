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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import cascading.tuple.Tuple;

public class SimpleDBOutputFormat implements OutputFormat<NullWritable, Tuple> {
    
    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
        // Do nothing - deprecated
    }

    @Override
    public RecordWriter<NullWritable, Tuple> getRecordWriter(FileSystem fs, JobConf conf, String name, Progressable progressable) throws IOException {
        return new SimpleDBRecordWriter(new SimpleDBConfiguration(conf));
    }

}
