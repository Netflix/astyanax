package com.netflix.astyanax.mapping;

import junit.framework.Assert;
import org.junit.Test;

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
}
