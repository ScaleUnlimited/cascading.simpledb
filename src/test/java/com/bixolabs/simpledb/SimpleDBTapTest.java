package com.bixolabs.simpledb;

import junit.framework.Assert;

import org.junit.Test;

import cascading.tap.SinkMode;
import cascading.tuple.Fields;


public class SimpleDBTapTest {

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
}
