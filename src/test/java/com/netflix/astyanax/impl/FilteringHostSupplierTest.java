package com.netflix.astyanax.impl;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.Host;

public class FilteringHostSupplierTest {
    public static class TestHostSupplier implements Supplier<List<Host>> {
        private List<Host> hostList;
        private boolean bThrowException = false;
        
        @Override
        public List<Host> get() {
            if (bThrowException) {
                throw new RuntimeException("Unknown exception");
            }
            return hostList;
        }

        public List<Host> getHostList() {
            return hostList;
        }

        public void setHostList(List<Host> hostList) {
            this.hostList = hostList;
        }

        public boolean isThrowException() {
            return bThrowException;
        }

        public void setThrowException(boolean bThrowException) {
            this.bThrowException = bThrowException;
        }
        
    }
    
    @Test
    public void testFilter() {
        List<Host> list1 = Arrays.asList(
                new Host("127.0.1.1", 7102),
                new Host("127.0.1.2", 7102),
                new Host("127.0.1.3", 7102)
                );
        
        List<Host> list2 = Arrays.asList(
                new Host("127.0.2.1", 7102),
                new Host("127.0.2.2", 7102),
                new Host("127.0.2.3", 7102)
                );

        List<Host> emptyList = Lists.newArrayList();
        
        TestHostSupplier discoverySupplier = new TestHostSupplier();
        TestHostSupplier ringSupplier = new TestHostSupplier();
        
        FilteringHostSupplier filteringSupplier = new FilteringHostSupplier(ringSupplier, discoverySupplier);
        
        List<Host> result;
        
        // Discovery only, no exception
        discoverySupplier.setHostList(list1);
        discoverySupplier.setThrowException(false);
        ringSupplier.setHostList(emptyList);
        ringSupplier.setThrowException(false);
        
        result = filteringSupplier.get();
        System.out.println("Discovery only, no exception : " + result);
        Assert.assertEquals(list1, result);
        
        // Ring only, no exception
        discoverySupplier.setHostList(emptyList);
        discoverySupplier.setThrowException(false);
        ringSupplier.setHostList(list1);
        ringSupplier.setThrowException(false);
        
        result = filteringSupplier.get();
        System.out.println("Ring only, no exception     :  " + result);
        Assert.assertEquals(emptyList, result);
        
        // Discovery and ring match
        discoverySupplier.setHostList(list1);
        discoverySupplier.setThrowException(false);
        ringSupplier.setHostList(list1);
        ringSupplier.setThrowException(false);
        
        result = filteringSupplier.get();
        System.out.println("Discovery and ring match     : " + result);
        Assert.assertEquals(list1, result);
        
        // Discovery and ring complete mismatch
        discoverySupplier.setHostList(list1);
        discoverySupplier.setThrowException(false);
        ringSupplier.setHostList(list2);
        ringSupplier.setThrowException(false);
        
        result = filteringSupplier.get();
        System.out.println("Discovery and ring mismatch  : " + result);
        Assert.assertEquals(list1, result);
        
        // Discovery OK, ring exception
        discoverySupplier.setHostList(list1);
        discoverySupplier.setThrowException(false);
        ringSupplier.setHostList(emptyList);
        ringSupplier.setThrowException(true);
        
        result = filteringSupplier.get();
        System.out.println("Discovery OK, ring exception : " + result);
        Assert.assertEquals(list1, result);

        // Discovery exception, ring ok
        discoverySupplier.setHostList(list1);
        discoverySupplier.setThrowException(true);
        ringSupplier.setHostList(list2);
        ringSupplier.setThrowException(false);
        
        result = filteringSupplier.get();
        System.out.println("Discovery exception, ring ok : " + result);
        Assert.assertEquals(emptyList, result);
        
        // Both empty
        discoverySupplier.setHostList(emptyList);
        discoverySupplier.setThrowException(false);
        ringSupplier.setHostList(emptyList);
        ringSupplier.setThrowException(false);
        
        result = filteringSupplier.get();
        System.out.println("Both empty                   : " + result);
        Assert.assertEquals(emptyList, result);
        
        // Both exception
        discoverySupplier.setHostList(list1);
        discoverySupplier.setThrowException(true);
        ringSupplier.setHostList(list2);
        ringSupplier.setThrowException(true);
        
        result = filteringSupplier.get();
        System.out.println("Bath exception               : " + result);
        Assert.assertEquals(emptyList, result);
    }
}
