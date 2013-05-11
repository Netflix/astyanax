package com.netflix.astyanax.entitystore;

import java.lang.String;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.google.common.base.Optional;

@Entity
public class CustomSerializerIdEntity {
    @SuppressWarnings("unused")
    @Serializer(OptionalUUIDSerializer.class)
    @Id
    private Optional<UUID> id;

    @SuppressWarnings("unused")
    @Column(name="test_column")
    private String testColumn;

    public Optional<UUID> getId() {
        return id;
    }

    public void setId(final Optional<UUID> id) {
        this.id = id;
    }

    public String getTestColumn() {
        return testColumn;
    }

    public void setTestColumn(final String testColumn) {
        this.testColumn = testColumn;
    }
}