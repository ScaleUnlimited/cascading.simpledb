package com.bixolabs.simpledb;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.HttpServer;
import org.mortbay.http.handler.AbstractHttpHandler;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;


public class SimpleDBTapTest {
    private static final Logger LOGGER = Logger.getLogger(SimpleDBTapTest.class);
    
    @SuppressWarnings("serial")
    private static class SdbResponseHandler extends AbstractHttpHandler {

        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
            LOGGER.info(String.format("%s: %s with params %s", request.getMethod(), pathInContext, pathParams));
            response.setContentLength(0);
            response.setContentType("text/html");
            response.setStatus(200);
        }
    }
    
    private HttpServer startServer(AbstractHttpHandler handler, int port) throws Exception {
        HttpServer server = new HttpServer();
        server.addListener(":" + port);
        HttpContext context = server.getContext("/");
        context.addHandler(handler);
        server.start();
        return server;
    }

    private Lfs makeSourceTuples(String path, Fields fields, int numTuples) throws IOException {
        Lfs lfs = new Lfs(new SequenceFile(fields), path, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        
        for (int i = 0; i < numTuples; i++) {
            Tuple t = new Tuple();
            for (int j = 0; j < fields.size(); j++) {
                String value = fields.get(j).toString() + "-" + i;
                t.add(value);
            }
            
            write.add(t);
        }
        
        write.close();
        return lfs;
    }

    @Test
    public void testEquality() throws Exception {
        final int numShards = 1;
        SimpleDBScheme scheme1 = new SimpleDBScheme(new Fields("a"), new Fields("a"));
        SimpleDBTap tap1 = new SimpleDBTap(scheme1, "accessKey", "secretKey", "baseDomainName", numShards, SinkMode.KEEP);
        
        SimpleDBTap tap2 = new SimpleDBTap(scheme1, "accessKey", "secretKey", "baseDomainName", numShards, SinkMode.KEEP);
        SimpleDBTap tap3 = new SimpleDBTap(scheme1, "accessKey", "secretKey", "baseDomainName", numShards, SinkMode.UPDATE);

        Assert.assertTrue(tap1.equals(tap2));
        Assert.assertFalse(tap1.equals(tap3));
    }
    
    @Test
    public void testMaxThreads() throws Exception {
        final int numShards = 1;
        SimpleDBScheme scheme = new SimpleDBScheme(new Fields("a"), new Fields("a"));
        SimpleDBTap tap = new SimpleDBTap(scheme, "accessKey", "secretKey", "baseDomainName", numShards, SinkMode.KEEP);
        
        Assert.assertEquals(numShards, tap.getMaxThreads());
        
        tap.setMaxThreads(10);
        Assert.assertEquals(10, tap.getMaxThreads());
    }
    
    // TODO KKr - reenable when issue with Jetty failing is resolved.
    // @Test
    public void testTooBusyHandling() throws Exception {
        HttpServer server = null;
        
        try {
            server = startServer(new SdbResponseHandler(), 8089);
            
            final int numShards = 10;
            final int numRecords = numShards * 30;
            final Fields testFields = new Fields("key", "value");

            final String in = "build/test/SimpleDBTapTest/testTooBusyHandling/in";
            Lfs lfsSource = makeSourceTuples(in, testFields, numRecords);

            SimpleDBScheme scheme = new SimpleDBScheme(testFields, new Fields("key"));

            Pipe pipe = new Pipe("test");
            final String tableName = "test";

            SimpleDBTap sink = new SimpleDBTap(scheme, "accessKey", "secretKey", tableName, numShards);
            sink.setSdbHost("localhost:8089");
            
            Flow flow = new FlowConnector().connect(lfsSource, sink, pipe);
            flow.complete();
        } catch (Exception e) {
            LOGGER.error("Exception during test", e);
            Assert.fail(e.getMessage());
        } finally {
            if (server != null) {
                server.stop();
            }
        }
    }
}
