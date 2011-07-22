package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.atomic.AtomicReference;

public class ThriftSyncConnectionFactoryImpl extends ThriftConnectionFactoryImpl<Cassandra.Client, ThriftSyncConnectionFactoryImpl.ConnectionData> {
    private static final String NAME_FORMAT = "ThriftConnection<%s-%d>";

    static class ConnectionData
    {
        private final TFramedTransport transport;

        ConnectionData(TFramedTransport transport) {
            this.transport = transport;
        }
    }

    public ThriftSyncConnectionFactoryImpl(ConnectionPoolConfiguration config) {
        super(config, NAME_FORMAT);
    }

    @Override
    protected void setKeyspace(Cassandra.Client client, String keyspaceName) throws TException, InvalidRequestException {
        client.set_keyspace(keyspaceName);
    }

    @Override
    protected Cassandra.Client createClient(HostConnectionPool<Cassandra.Client> pool, AtomicReference<ThriftSyncConnectionFactoryImpl.ConnectionData> connectionDataRef) throws ConnectionException {
        ConnectionData      connectionData;
        TSocket socket;
        try {
            socket = new TSocket(pool.getHost().getIpAddress(),
                    pool.getHost().getPort(), config.getSocketTimeout());

            connectionData = new ConnectionData(new TFramedTransport(socket));
            connectionData.transport.open();
            connectionDataRef.set(connectionData);
        }
        catch (TTransportException e) {
            // Thrift exceptions aren't very good in reporting, so we have to catch the exception here and
            // add details to it.
            throw new TransportException("Failed to open transport", e);	// TODO
        }

        return new Cassandra.Client(new TBinaryProtocol(connectionData.transport));
    }

    @Override
    protected void closeClient(Cassandra.Client client, ThriftSyncConnectionFactoryImpl.ConnectionData connectionData) {
        try {
        	if (connectionData != null && connectionData.transport != null)
        		connectionData.transport.flush();
        } catch (TTransportException e) {
            // ignore
        }
        finally {
        	if (connectionData != null && connectionData.transport != null)
        		connectionData.transport.close();
        }
    }

    @Override
    protected boolean clientIsOpen(Cassandra.Client client, ThriftSyncConnectionFactoryImpl.ConnectionData connectionData) {
        return connectionData != null && connectionData.transport != null && connectionData.transport.isOpen();
    }
}
