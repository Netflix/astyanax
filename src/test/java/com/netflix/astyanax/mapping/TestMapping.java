package com.netflix.astyanax.mapping;

import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import junit.framework.Assert;

@SuppressWarnings("deprecation")
public class TestMapping {

    @Test
    public void testKeyspaceAnnotations() {
        FakeKeyspaceBean override = new FakeKeyspaceBean();
        override.setId("1");
        override.setCountry("USA");
        override.setCountryStatus(2);
        override.setCreateTS(12345678L);
        override.setExpirationTS(87654321L);
        override.setLastUpdateTS(24681357L);
        override.setType("thing");
        override.setUpdatedBy("John Galt");
        override.setByteArray("Some Bytes".getBytes());

        Mapping<FakeKeyspaceBean> mapping = Mapping
                .make(FakeKeyspaceBean.class);

        Assert.assertEquals(mapping.getIdValue(override, String.class),
                override.getId());
        Assert.assertEquals(
                mapping.getColumnValue(override, "PK", String.class),
                override.getId());
        Assert.assertEquals(mapping.getColumnValue(override,
                "COUNTRY_OVERRIDE", String.class), override.getCountry());
        Assert.assertEquals(mapping.getColumnValue(override,
                "COUNTRY_STATUS_OVERRIDE", Integer.class), override
                .getCountryStatus());
        Assert.assertEquals(
                mapping.getColumnValue(override, "CREATE_TS", Long.class),
                override.getCreateTS());
        Assert.assertEquals(
                mapping.getColumnValue(override, "EXP_TS", Long.class),
                override.getExpirationTS());
        Assert.assertEquals(
                mapping.getColumnValue(override, "LAST_UPDATE_TS", Long.class),
                override.getLastUpdateTS());
        Assert.assertEquals(mapping.getColumnValue(override,
                "OVERRIDE_BY_TYPE", String.class), override.getType());
        Assert.assertEquals(
                mapping.getColumnValue(override, "UPDATED_BY", String.class),
                override.getUpdatedBy());
        Assert.assertEquals(
                mapping.getColumnValue(override, "BYTE_ARRAY", byte[].class),
                override.getByteArray());

        FakeKeyspaceBean copy = new FakeKeyspaceBean();
        for (String fieldName : mapping.getNames()) {
            mapping.setColumnValue(copy, fieldName,
                    mapping.getColumnValue(override, fieldName, Object.class));
        }

        Assert.assertEquals(copy.getId(), override.getId());
        Assert.assertEquals(copy.getCountry(), override.getCountry());
        Assert.assertEquals(copy.getCountryStatus(),
                override.getCountryStatus());
        Assert.assertEquals(copy.getCreateTS(), override.getCreateTS());
        Assert.assertEquals(copy.getExpirationTS(), override.getExpirationTS());
        Assert.assertEquals(copy.getLastUpdateTS(), override.getLastUpdateTS());
        Assert.assertEquals(copy.getType(), override.getType());
        Assert.assertEquals(copy.getUpdatedBy(), override.getUpdatedBy());
        Assert.assertEquals(copy.getByteArray(), override.getByteArray());
    }

    @Test
    public void testCache() {
        MappingCache cache = new MappingCache();

        Mapping<FakeKeyspaceBean> keyspaceBeanMapping1 = cache
                .getMapping(FakeKeyspaceBean.class);
        Mapping<FakeKeyspaceBean> keyspaceBeanMapping2 = cache
                .getMapping(FakeKeyspaceBean.class);

        Assert.assertSame(keyspaceBeanMapping1, keyspaceBeanMapping2);
    }

    @Test
    public void testTtlMapping() {
        TtlTestBean testBean = new TtlTestBean();
        testBean.setId("1");
        testBean.setDefaultTtl("defaultval");
        testBean.setZeroTtl("zeroval");
        testBean.setSixtyTtl("sixtyval");

        Mapping<TtlTestBean> mapping = Mapping.make(TtlTestBean.class);

        ColumnListMutationImplementation mutation = new ColumnListMutationImplementation();
        mapping.fillMutation(testBean, mutation);
        Assert.assertEquals(null, mutation.getTtls().get("defaultval"));
        Assert.assertEquals(new Integer(0), mutation.getTtls().get("zeroval"));
        Assert.assertEquals(new Integer(60), mutation.getTtls().get("sixtyval"));
    }

    @Test
    public void testTtlMappingWithDefaultExpiry() {
        TtlTestBean testBean = new TtlTestBean();
        testBean.setId("1");
        testBean.setDefaultTtl("defaultval");
        testBean.setZeroTtl("zeroval");
        testBean.setSixtyTtl("sixtyval");

        Mapping<TtlTestBean> mapping = Mapping.make(TtlTestBean.class,
                new DefaultAnnotationSet(new Integer(33)));

        ColumnListMutationImplementation mutation = new ColumnListMutationImplementation();
        mapping.fillMutation(testBean, mutation);
        Assert.assertEquals(new Integer(33),
                mutation.getTtls().get("defaultval"));
        Assert.assertEquals(new Integer(0), mutation.getTtls().get("zeroval"));
        Assert.assertEquals(new Integer(60), mutation.getTtls().get("sixtyval"));
    }

    private final class ColumnListMutationImplementation implements
            ColumnListMutation<String> {
        final Map<String, Integer> ttls = new HashMap<String, Integer>();

        public Map<String, Integer> getTtls() {
            return ttls;
        }

        @Override
        public <V> ColumnListMutation<String> putColumn(String columnName,
                V value, Serializer<V> valueSerializer, Integer ttl) {
            rememberTtl(value, ttl);
            return this;
        }

        private <V> void rememberTtl(V value, Integer ttl) {
            ttls.put(value.toString(), ttl);
        }

        @SuppressWarnings("deprecation")
        @Override
        public <SC> ColumnListMutation<SC> withSuperColumn(
                ColumnPath<SC> superColumnPath) {
            return null;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                String value, Integer ttl) {
            rememberTtl(value, ttl);
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                String value) {
            rememberTtl(value, null);
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                byte[] value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                byte[] value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                int value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName, int value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                long value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                long value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                boolean value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                boolean value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                ByteBuffer value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                ByteBuffer value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                Date value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                Date value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                float value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                float value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                double value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                double value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                UUID value, Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putColumn(String columnName,
                UUID value) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putEmptyColumn(String columnName,
                Integer ttl) {
            return this;
        }

        @Override
        public ColumnListMutation<String> putEmptyColumn(String columnName) {
            return this;
        }

        @Override
        public ColumnListMutation<String> incrementCounterColumn(
                String columnName, long amount) {
            return this;
        }

        @Override
        public ColumnListMutation<String> deleteColumn(String columnName) {
            return this;
        }

        @Override
        public ColumnListMutation<String> setTimestamp(long timestamp) {
            return this;
        }

        @Override
        public ColumnListMutation<String> delete() {
            return this;
        }

        @Override
        public ColumnListMutation<String> setDefaultTtl(Integer ttl) {
            return this;
        }
    }

}
