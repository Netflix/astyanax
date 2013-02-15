/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionAbortedException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.ThriftStateException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TokenRangeOfflineException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ColumnType;
import com.netflix.astyanax.model.ConsistencyLevel;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class ThriftConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftConverter.class);

    /**
     * Construct a Hector ColumnParent based on the information in the query and
     * the type of column family being queried.
     * 
     * @param <K>
     * @param columnFamily
     * @param path
     * @return
     * @throws BadRequestException
     */
    public static <K> ColumnParent getColumnParent(ColumnFamily<?, ?> columnFamily, ColumnPath<?> path)
            throws BadRequestException {
        ColumnParent cp = new ColumnParent();
        cp.setColumn_family(columnFamily.getName());
        if (path != null) {
            Iterator<ByteBuffer> columns = path.iterator();
            if (columnFamily.getType() == ColumnType.SUPER && columns.hasNext()) {
                cp.setSuper_column(columns.next());
            }
        }
        return cp;
    }

    /**
     * Construct a Thrift ColumnPath based on the information in the query and
     * the type of column family being queried.
     * 
     * @param <K>
     * @param columnFamily
     * @param path
     * @return
     * @throws NotFoundException
     * @throws InvalidRequestException
     * @throws TException
     */
    public static <K> org.apache.cassandra.thrift.ColumnPath getColumnPath(ColumnFamily<?, ?> columnFamily,
            ColumnPath<?> path) throws BadRequestException {
        org.apache.cassandra.thrift.ColumnPath cp = new org.apache.cassandra.thrift.ColumnPath();
        cp.setColumn_family(columnFamily.getName());
        if (path != null) {
            Iterator<ByteBuffer> columns = path.iterator();
            if (columnFamily.getType() == ColumnType.SUPER && columns.hasNext()) {
                cp.setSuper_column(columns.next());
            }

            if (columns.hasNext()) {
                cp.setColumn(columns.next());
            }
            if (columns.hasNext()) {
                throw new BadRequestException("Path depth of " + path.length() + " not supported for column family \'"
                        + columnFamily.getName() + "\'");
            }
        }
        return cp;
    }

    /**
     * Return a Hector SlicePredicate based on the provided column slice
     * 
     * @param <C>
     * @param columns
     * @param colSer
     * @return
     */
    public static <C> SlicePredicate getPredicate(ColumnSlice<C> columns, Serializer<C> colSer) {
        // Get all the columns
        if (columns == null) {
            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]), false,
                    Integer.MAX_VALUE));
            return predicate;
        }
        // Get a specific list of columns
        if (columns.getColumns() != null) {
            SlicePredicate predicate = new SlicePredicate();
            predicate.setColumn_namesIsSet(true);
            predicate.column_names = colSer.toBytesList(columns.getColumns());
            return predicate;
        }
        else {
            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(new SliceRange((columns.getStartColumn() == null) ? ByteBuffer.wrap(new byte[0])
                    : ByteBuffer.wrap(colSer.toBytes(columns.getStartColumn())),
                    (columns.getEndColumn() == null) ? ByteBuffer.wrap(new byte[0]) : ByteBuffer.wrap(colSer
                            .toBytes(columns.getEndColumn())), columns.getReversed(), columns.getLimit()));
            return predicate;
        }
    }

    /**
     * Convert from Thrift exceptions to an internal ConnectionPoolException
     * 
     * @param e
     * @return
     */
    public static ConnectionException ToConnectionPoolException(Throwable e) {
        if (e instanceof ConnectionException) {
            return (ConnectionException) e;
        }
        LOGGER.debug(e.getMessage());
        if (e instanceof InvalidRequestException) {
            return new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(e);
        }
        else if (e instanceof TProtocolException) {
            return new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(e);
        }
        else if (e instanceof UnavailableException) {
            return new TokenRangeOfflineException(e);
        }
        else if (e instanceof SocketTimeoutException) {
            return new TimeoutException(e);
        }
        else if (e instanceof TimedOutException) {
            return new OperationTimeoutException(e);
        }
        else if (e instanceof NotFoundException) {
            return new com.netflix.astyanax.connectionpool.exceptions.NotFoundException(e);
        }
        else if (e instanceof TApplicationException) {
            return new ThriftStateException(e);
        }
        else if (e instanceof AuthenticationException || e instanceof AuthorizationException) {
            return new com.netflix.astyanax.connectionpool.exceptions.AuthenticationException(e);
        }
        else if (e instanceof SchemaDisagreementException) {
            return new com.netflix.astyanax.connectionpool.exceptions.SchemaDisagreementException(e);
        }
        else if (e instanceof TTransportException) {
            if (e.getCause() != null) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    return new TimeoutException(e);
                }
                if (e.getCause().getMessage() != null) {
                    if (e.getCause().getMessage().toLowerCase().contains("connection abort")
                            || e.getCause().getMessage().toLowerCase().contains("connection reset")) {
                        return new ConnectionAbortedException(e);
                    }
                }
            }
            return new TransportException(e);
        }
        else {
            // e.getCause().printStackTrace();
            return new UnknownException(e);
        }
    }

    public static org.apache.cassandra.thrift.ConsistencyLevel ToThriftConsistencyLevel(ConsistencyLevel cl) {
        switch (cl) {
        case CL_ONE:
            return org.apache.cassandra.thrift.ConsistencyLevel.ONE;
        case CL_QUORUM:
            return org.apache.cassandra.thrift.ConsistencyLevel.QUORUM;
        case CL_EACH_QUORUM:
            return org.apache.cassandra.thrift.ConsistencyLevel.EACH_QUORUM;
        case CL_LOCAL_QUORUM:
            return org.apache.cassandra.thrift.ConsistencyLevel.LOCAL_QUORUM;
        case CL_TWO:
            return org.apache.cassandra.thrift.ConsistencyLevel.TWO;
        case CL_THREE:
            return org.apache.cassandra.thrift.ConsistencyLevel.THREE;
        case CL_ALL:
            return org.apache.cassandra.thrift.ConsistencyLevel.ALL;
        case CL_ANY:
            return org.apache.cassandra.thrift.ConsistencyLevel.ANY;
        default:
            return org.apache.cassandra.thrift.ConsistencyLevel.QUORUM;
        }
    }
}
