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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientParamBean;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.AbstractVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.cookie.params.CookieSpecParamBean;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

public class BackoffHttpHandler implements IHttpHandler {
    private static final Logger LOGGER = Logger.getLogger(BackoffHttpHandler.class);
    
    private static final int SOCKET_TIMEOUT = 30 * 1000;
    private static final int CONNECTION_TIMEOUT = 30 * 1000;
    private static final long CONNECTION_POOL_TIMEOUT = 100 * 1000L;
    
    private static final int BUFFER_SIZE = 8 * 1024;

    private static final int DEFAULT_MAX_THREADS = 100;
    private static final String USER_AGENT = "Cascading SimpleDB Tap";
    private static final int MAX_HTTP_REDIRECTS = 1;
    private static final int MAX_HTTP_RETRIES = 5;

    private static final long MAX_AWS_BACKOFF = 100000L;
    private static final double AWS_BACKOFF_RANDOM_PERCENT = 0.2;
    private static final int MAX_AWS_RETRIES = 10;
    
    private static final String SSL_CONTEXT_NAMES[] = {
        "TLS",
        "Default",
        "SSL",
    };

    private static class MyRequestRetryHandler implements HttpRequestRetryHandler {
        private int _maxRetryCount;
        
        public MyRequestRetryHandler(int maxRetryCount) {
            _maxRetryCount = maxRetryCount;
        }
        
        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Decide about retry #" + executionCount + " for exception " + exception.getMessage());
            }
            
            if (executionCount >= _maxRetryCount) {
                // Do not retry if over max retry count
                return false;
            } else if (exception instanceof NoHttpResponseException) {
                // Retry if the server dropped connection on us
                return true;
            } else if (exception instanceof SSLHandshakeException) {
                // Do not retry on SSL handshake exception
                return false;
            }
            
            HttpRequest request = (HttpRequest)context.getAttribute(ExecutionContext.HTTP_REQUEST);
            boolean idempotent = !(request instanceof HttpEntityEnclosingRequest); 
            // Retry if the request is considered idempotent 
            return idempotent;
        }
    }

    private static class DummyX509TrustManager implements X509TrustManager {
        private X509TrustManager standardTrustManager = null;

        public DummyX509TrustManager(KeyStore keystore) throws NoSuchAlgorithmException, KeyStoreException {
            super();
            
            String algo = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory factory = TrustManagerFactory.getInstance(algo);
            factory.init(keystore);
            TrustManager[] trustmanagers = factory.getTrustManagers();
            if (trustmanagers.length == 0) {
                throw new NoSuchAlgorithmException(algo + " trust manager not supported");
            }
            this.standardTrustManager = (X509TrustManager)trustmanagers[0];
        }

        public boolean isClientTrusted(X509Certificate[] certificates) {
            return true;
        }

        public boolean isServerTrusted(X509Certificate[] certificates) {
            return true;
        }

        public X509Certificate[] getAcceptedIssuers() {
            return this.standardTrustManager.getAcceptedIssuers();
        }

        public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
            // do nothing
        }

        public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
            // do nothing
        }
    }

    private static class DummyX509HostnameVerifier extends AbstractVerifier {

        @Override
        public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
            try {
                verify(host, cns, subjectAlts, false);
            } catch (SSLException e) {
                LOGGER.warn("Invalid SSL certificate for " + host + ": " + e.getMessage());
            }
        }
        
        @Override
        public final String toString() { 
            return "DUMMY_VERIFIER"; 
        }
    }
    
    private DefaultHttpClient _httpClient;
    private Random _random;
    
    public BackoffHttpHandler() {
        this(DEFAULT_MAX_THREADS);
    }

    public BackoffHttpHandler(int maxThreads) {
        _httpClient = createClient(maxThreads);
        _random = new Random(System.currentTimeMillis());
    }

    @Override
    public String get(URL url) throws IOException, HttpException, InterruptedException {
        return doRequestWithRetries(new HttpGet(), url);
    }
    
    @Override
    public String post(URL url, Map<String, String> params) throws IOException, HttpException, InterruptedException {
        HttpPost request = new HttpPost();
        request.setHeader("Host", url.getHost());
        
        StringBuilder body = new StringBuilder();
        List<String> keys = new ArrayList<String>(params.keySet());
        Collections.sort(keys);
        
        Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String key = it.next();
            String val = params.get(key);

            body.append(key);
            body.append("=");
            body.append(URLEncoder.encode(val, "utf-8").replace("+", "%20").replace("*", "%2A").replace("%7E","~"));

            if (it.hasNext()) {
                body.append("&");
            }
        }

        StringEntity entity = new StringEntity(body.toString(), "UTF-8");
        entity.setContentType("application/x-www-form-urlencoded; charset=utf-8");
        request.setEntity(entity);
        
        try {
            return doRequestWithRetries(request, url);
        } catch (HttpException e) {
            if ((e.getStatusCode() == 403) && LOGGER.isTraceEnabled()) {
                LOGGER.trace("Authentication error with post: " + body.toString());
            }
            
            throw e;
        }
    }

    private String doRequestWithRetries(HttpRequestBase request, URL url) throws IOException, HttpException, InterruptedException {
        int numRetries = 0;
        
        while (true) {
            try {
                return doRequest(request, url);
            } catch (HttpException e) {
                int statusCode = e.getStatusCode();
                if ((statusCode == 500) || (statusCode == 503) || (statusCode == 408)) {
                    numRetries += 1;
                    if (numRetries > MAX_AWS_RETRIES) {
                        throw e;
                    } else {
                        // Calculate an increasing delay, capped at a max value, that randomly varies so we don't
                        // keep re-hitting the server at roughly the same time.
                        double targetDelay = Math.min(Math.pow(4.0, numRetries) * 100L, MAX_AWS_BACKOFF);
                        long delay = (long)(targetDelay * (1.0 + (_random.nextDouble() * AWS_BACKOFF_RANDOM_PERCENT)));
                        LOGGER.debug("Retriable error detected, will retry in " + delay + "ms, attempt number: " + numRetries);
                        Thread.sleep(delay);
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    private String doRequest(HttpRequestBase request, URL url) throws IOException, HttpException, InterruptedException {
        boolean needAbort = true;
        InputStream in = null;
        String content = "";
        
        try {
            request.setURI(url.toURI());
            
            HttpContext localContext = new BasicHttpContext();
            CookieStore cookieStore = new BasicCookieStore();
            localContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);

            HttpResponse response = _httpClient.execute(request, localContext);
            int httpStatus = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead = 0;
                ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
                in = entity.getContent();

                while ((bytesRead = in.read(buffer, 0, buffer.length)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
                
                content = new String(out.toByteArray(), "UTF-8");
            }
            
            // We've read everything in, so we're all good.
            needAbort = false;
            
            if (httpStatus >= 300) {
                throw new HttpException(httpStatus, content);
            }
        } catch (IOException e) {
            // Oleg guarantees that no abort is needed in the case of an IOException
            needAbort = false;
        } catch (URISyntaxException e) {
            needAbort = false;
            throw new MalformedURLException("Can't convert URL to URI: " + url);
        } finally {
            safeClose(in);
            safeAbort(needAbort, request);
        }

        
        return content;
    }

    private DefaultHttpClient createClient(int maxThreads) {
        // Create and initialize HTTP parameters
        HttpParams params = new BasicHttpParams();

        // TODO KKr - w/4.1, switch to new api (ThreadSafeClientConnManager)
        // cm.setMaxTotalConnections(_maxThreads);
        // cm.setDefaultMaxPerRoute(Math.max(10, _maxThreads/10));
        ConnManagerParams.setMaxTotalConnections(params, maxThreads);
        
        // Set the maximum time we'll wait for a spare connection in the connection pool. We
        // shouldn't actually hit this, as we make sure (in FetcherManager) that the max number
        // of active requests doesn't exceed the value returned by getMaxThreads() here.
        ConnManagerParams.setTimeout(params, CONNECTION_POOL_TIMEOUT);
        
        // Set the socket and connection timeout to be something reasonable.
        HttpConnectionParams.setSoTimeout(params, SOCKET_TIMEOUT);
        HttpConnectionParams.setConnectionTimeout(params, CONNECTION_TIMEOUT);
        
        // Even with stale checking enabled, a connection can "go stale" between the check and the
        // next request. So we still need to handle the case of a closed socket (from the server side),
        // and disabling this check improves performance.
        HttpConnectionParams.setStaleCheckingEnabled(params, false);
        
        ConnPerRouteBean connPerRoute = new ConnPerRouteBean(maxThreads);
        ConnManagerParams.setMaxConnectionsPerRoute(params, connPerRoute);

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setUserAgent(params, USER_AGENT);
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        HttpProtocolParams.setHttpElementCharset(params, "UTF-8");
        HttpProtocolParams.setUseExpectContinue(params, true);

        // TODO KKr - set on connection manager params, or client params?
        CookieSpecParamBean cookieParams = new CookieSpecParamBean(params);
        cookieParams.setSingleHeader(true);

        // Create and initialize scheme registry
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
        SSLSocketFactory sf = null;

        for (String contextName : SSL_CONTEXT_NAMES) {
            try {
                SSLContext sslContext = SSLContext.getInstance(contextName);
                sslContext.init(null, new TrustManager[] { new DummyX509TrustManager(null) }, null);
                sf = new SSLSocketFactory(sslContext);
                break;
            } catch (NoSuchAlgorithmException e) {
                LOGGER.debug("SSLContext algorithm not available: " + contextName);
            } catch (Exception e) {
                LOGGER.debug("SSLContext can't be initialized: " + contextName, e);
            }
        }
        
        if (sf != null) {
            sf.setHostnameVerifier(new DummyX509HostnameVerifier());
            schemeRegistry.register(new Scheme("https", sf, 443));
        } else {
            LOGGER.warn("No valid SSLContext found for https");
        }


        // Use ThreadSafeClientConnManager since more than one thread will be using the HttpClient.
        ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
        DefaultHttpClient result = new DefaultHttpClient(cm, params);
        result.setHttpRequestRetryHandler(new MyRequestRetryHandler(MAX_HTTP_RETRIES));
        
        params = result.getParams();
        HttpClientParams.setAuthenticating(params, false);
        HttpClientParams.setCookiePolicy(params, CookiePolicy.BROWSER_COMPATIBILITY);
        
        ClientParamBean clientParams = new ClientParamBean(params);
        clientParams.setHandleRedirects(true);
        clientParams.setMaxRedirects(MAX_HTTP_REDIRECTS);
        
        return result;
    }
    
    private void safeAbort(boolean needAbort, HttpRequestBase request) {
        if (needAbort && (request != null)) {
            try {
                request.abort();
            } catch (Throwable t) {
                // Ignore any errors
            }
        }
    }

    private void safeClose(Closeable o) {
        if (o != null) {
            try {
                o.close();
            } catch (Throwable t) {
                // Ignore any errors
            }
        }
    }
}
