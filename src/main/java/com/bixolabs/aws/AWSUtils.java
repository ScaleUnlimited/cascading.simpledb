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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SignatureException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class AWSUtils {
	private static final String DATEFORMAT_AWS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

	/**
	 * Generate a timestamp for use with AWS request signing
	 *
	 * @param date current date
	 * @return timestamp
	 */
	public static String getTimestampFromLocalTime(Date date) {
		SimpleDateFormat format = new SimpleDateFormat(DATEFORMAT_AWS);
		format.setTimeZone(TimeZone.getTimeZone("GMT"));
		return format.format(date);
	}

	/**
	 * Computes RFC 2104-compliant HMAC signature.
	 *
	 * @param data
	 *            The data to be signed.
	 * @param key
	 *            The signing key.
	 * @return The base64-encoded RFC 2104-compliant HMAC signature.
	 * @throws java.security.SignatureException
	 *             when signature generation fails
	 */
	public static String generateSignature(String data, String key) throws SignatureException {
		try {
			// get an hmac_sha1 key from the raw key bytes
			SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(), HMAC_SHA1_ALGORITHM);

			// get an hmac_sha1 Mac instance and initialize with the signing key
			Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
			mac.init(signingKey);

			// compute the hmac on input data bytes
			byte[] rawHmac = mac.doFinal(data.getBytes());

			// base64-encode the hmac
			return Base64.encodeBytes(rawHmac);

		} catch (Exception e) {
			throw new SignatureException("Failed to generate HMAC : "
					+ e.getMessage());
		}
	}

	private static Properties getProperties(String propFilename) throws IOException {
        Properties props = new Properties();
        InputStream is;
        
        if (!propFilename.startsWith("/")) {
            is = AWSUtils.class.getResourceAsStream("/" + propFilename);
            if (is == null) {
                throw new IllegalArgumentException("Can't find AWS keys file on path: " + propFilename);
            }
        } else {
            File propFile = new File(propFilename);
            is = new FileInputStream(propFile);
        }
        
        props.load(is);
        return props;
	}
	
    public static String getAccessKeyID(String propFilename) throws IOException {
        return getProperties(propFilename).getProperty("access-key-id");
    }
    
    public static String getSecretAccessKey(String propFilename) throws IOException {
        return getProperties(propFilename).getProperty("secret-access-key");
    }


}
