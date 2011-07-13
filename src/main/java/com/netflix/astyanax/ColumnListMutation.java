package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.model.ColumnPath;


/**
 * Abstraction for inserting columns either at the root of a row or 
 * in a super column. 
 * @author elandau
 *
 * TODO: Composite putColumn
 * 
 * @param <C>
 */
public interface ColumnListMutation<C> {
	/**
	 * Generic call to insert a column value with a custom serializer.
	 * User this only when you need a customer serializer otherwise use
	 * the overloaded putColumn calls to insert common value types.
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @param valueSerializer
	 * @param ttl
	 * @return
	 */
	<V> ColumnListMutation<C> putColumn(C columnName, V value,
			Serializer<V> valueSerializer, Integer ttl);

	/**
	 * Generic call to insert a super column.  Notice that this call receives
	 * a column path which has a new serializer type for the sub column names.
	 * Also, the object returned by this call differs from the parent column
	 * and should therefore not be chained with any calls to add columns
	 * to this serializer.
	 * 
	 * @param <SC>
	 * @param superColumnPath
	 * @return
	 */
	<SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath);
	
	ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, Double value, Integer ttl);

	ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl);
	
	ColumnListMutation<C> incrementCounterColumn(C columnName, long amount);
	
	ColumnListMutation<C> deleteColumn(C columnName);
	
	/**
	 * Deletes all columns at the current column path location.  Delete at the
	 * root of a row effectively deletes the entire row.
	 * @return
	 */
	ColumnListMutation<C> delete();
}
