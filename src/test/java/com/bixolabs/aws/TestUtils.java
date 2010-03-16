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

import java.io.IOException;
import java.util.Properties;

public class TestUtils {
    public static final String TEST_DOMAIN_NAME = TestUtils.class.getSimpleName() + "-domain";
    
    public static String getAccessKeyID() throws IOException {
        Properties props = new Properties();
        props.load(TestUtils.class.getResourceAsStream("/aws-keys.txt"));
        return props.getProperty("access-key-id");
    }
    
    public static String getSecretAccessKey() throws IOException {
        Properties props = new Properties();
        props.load(TestUtils.class.getResourceAsStream("/aws-keys.txt"));
        return props.getProperty("secret-access-key");
    }
    

}
