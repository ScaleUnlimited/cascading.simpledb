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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class SimpleDBInputSplit implements InputSplit {

    private int _length;
    private int _selectLimit;
    private String _shardname;
    
    public SimpleDBInputSplit() {
        // Empty constructor for Writable support
    }
    
    public SimpleDBInputSplit(int length, String shardName) {
        this(length, SimpleDBUtils.NO_SELECT_LIMIT, shardName);
    }

    public SimpleDBInputSplit(int length, int selectLimit, String shardName) {
        _length = length;
        _selectLimit = selectLimit;
        _shardname = shardName;
    }

    @Override
    public long getLength() throws IOException {
        return _length;
    }

    public int getSelectLimit() throws IOException {
        return _selectLimit;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[]{ _shardname };
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _length = in.readInt();
        _selectLimit = in.readInt();
        _shardname = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(_length);
        out.writeInt(_selectLimit);
        out.writeUTF(_shardname);
    }

}
