package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;


public interface ColumnMap<C> extends Iterable<Column<C>> {
    /**
     * Return the underlying map
     * @return
     */
    Map<C, Column<C>> asMap();

    /**
     * Queries column by name
     *
     * @param columnName
     * @return an instance of a column or null if not found
     * @throws Exception
     */
    Column<C> get(C columnName);

    /**
     * Return value as a string
     *
     * @return
     */
    String getString(C columnName, String defaultValue);

    /**
     * Return value as an integer
     *
     * @return
     */
    Integer getInteger(C columnName, Integer defaultValue);

    /**
     * Return value as a double
     *
     * @return
     */
    Double getDouble(C columnName, Double defaultValue);

    /**
     * Return value as a long. Use this to get the value of a counter column
     *
     * @return
     */
    Long getLong(C columnName, Long defaultValue);

    /**
     * Get the raw byte[] value
     *
     * @return
     */
    byte[] getByteArray(C columnName, byte[] defaultValue);

    /**
     * Get value as a boolean
     *
     * @return
     */
    Boolean getBoolean(C columnName, Boolean defaultValue);

    /**
     * Get the raw ByteBuffer value
     *
     * @return
     */
    ByteBuffer getByteBuffer(C columnName, ByteBuffer defaultValue);

    /**
     * Get the value as a date object
     *
     * @return
     */
    Date getDate(C columnName, Date defaultValue);

    /**
     * Get the value as a UUID
     *
     * @return
     */
    UUID getUUID(C columnName, UUID defaultValue);

    /**
     * Indicates if the list of columns is empty
     *
     * @return
     */
    boolean isEmpty();

    /**
     * returns the number of columns in the row
     *
     * @return
     */
    int size();

    /**
     * Add a single column to the collection
     * @param column
     * @return
     */
    OrderedColumnMap<C> add(Column<C> column);

    /**
     * Add a set of columns to the collection
     * @param columns
     * @return
     */
    OrderedColumnMap<C> addAll(Collection<Column<C>> columns);
}
