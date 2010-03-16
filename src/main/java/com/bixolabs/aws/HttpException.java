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
public class HttpException extends Exception {

    private int _statusCode;
    private String _response;
    
    protected HttpException() { }

    protected HttpException(String message, Throwable cause) { }

    protected HttpException(String message) { }

    protected HttpException(Throwable cause) { }

    public HttpException(int statusCode, String response) {
        super();
        
        _statusCode = statusCode;
        _response = response;
    }

    public HttpException(int statusCode, String response, String msg) {
        super(msg);
        
        _statusCode = statusCode;
        _response = response;
    }

    public int getStatusCode() {
        return _statusCode;
    }

    public String getResponse() {
        return _response;
    }

    
}
