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

@SuppressWarnings("serial")
public class AWSException extends Exception {

    public static final String NO_AWS_ERROR_CODE = "NoAwsErrorCode";
    public static final String NO_SUCH_DOMAIN = "NoSuchDomain";
    
    private int _responseCode;
    private String _awsErrorCode;
    
    protected AWSException() {
    }

    protected AWSException(String message) {
        super(message);
    }

    protected AWSException(Throwable cause) {
        super(cause);
    }

    protected AWSException(String message, Throwable cause) {
        super(message, cause);
    }

    public AWSException(int responseCode, String awsErrorCode, String message) {
        super(message);
        _responseCode = responseCode;
        _awsErrorCode = awsErrorCode;
    }

    public AWSException(int responseCode, String awsErrorCode, String message, Throwable cause) {
        super(message, cause);
        _responseCode = responseCode;
        _awsErrorCode = awsErrorCode;
    }

    public int getResponseCode() {
        return _responseCode;
    }
    
    public String getAWSErrorCode() {
        return _awsErrorCode;
    }
}
