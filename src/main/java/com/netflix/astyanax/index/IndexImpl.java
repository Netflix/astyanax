package com.netflix.astyanax.index;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ObjectSerializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TypeInferringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Thread unsafe row based - high cardinality index 
 * 
 * The main idea is that "indexes" are stored in their own CF as
 * light weight composites with "pointers" back to the PK.
 * 
 * 
 * Can optionally store values as well, but not for this round.
 * 
 * 
 * @author marcus
 *
 * @param <N>
 * @param <V>
 * @param <K>
 */
/*
 drop column family index_cf;
 create column family index_cf;
 */
public class IndexImpl<N,V,K> implements Index<N, V, K>
{

	static Keyspace keyspace = null;
	static AstyanaxContext<Keyspace> context = null;
	private MutationBatch mutationBatch;
	
	
	private String cf;
	
	public IndexImpl() {
		
	}
	
	public IndexImpl(MutationBatch mutationBatch) {
		this.mutationBatch = mutationBatch;
	}
	
	//TODO
	//this should be moved out.
	//TODO
	public static void init() {
		context = new AstyanaxContext.Builder()
				.forCluster("ClusterName")
				.forKeyspace("icrskeyspace")
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(1)
								.setSeeds("127.0.0.1:9160"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		keyspace = context.getEntity();

	}
	
	
	@Override
	public void buildIndex(String cf,N colName,Class<K> keyType) throws ConnectionException {
		
		
				
		Serializer<K> keySerializer = SerializerTypeInferer.getSerializer(keyType);
		Serializer<N> colSerializer = SerializerTypeInferer.getSerializer(colName.getClass());
		 
		ColumnFamily<K, N> colFamily = new ColumnFamily<K, N>(cf,keySerializer, colSerializer);
		
		//Get all rows: 
		//TODO: danger to paginate
		MutationBatch m = keyspace.prepareMutationBatch();
		OperationResult<Rows<K,N>> result = keyspace.prepareQuery(colFamily)
				.getAllRows()
				.withColumnSlice(colName)
				.execute();
		Rows<K,N> rows = result.getResult();
		Iterator<Row<K,N>> iter =  rows.iterator();
		
		//The index column family
		CompositeSerializer compSerializer = CompositeSerializer.get();
		ColumnFamily<Composite, ByteBuffer> CF = new ColumnFamily<Composite, ByteBuffer>(
				"index_cf", compSerializer, ByteBufferSerializer.get());
		//byte []pkType = getType(value)
		//put them all in: no updates
		while (iter.hasNext()) {
			Row<K,N> row = iter.next();
			
			//the value of the column
			byte []value = row.getColumns().getColumnByName(colName).getByteArrayValue();
						
			
			Composite toInsert = new Composite(this.cf,colName,value);
			
			ByteBuffer buffer = TypeInferringSerializer.get().toByteBuffer(row.getKey());
			
			m.withRow(CF, toInsert).putColumn(buffer, getType(row.getKey()));
			
			
			
			
		}
		m.execute();
		
		
	}
	
	

	@Override
	public void deleteIndex(String cf, N name) throws ConnectionException {
		// TODO Auto-generated method stub
		
	}

	//@Override
	public void createIndex(String columnFamilyName) {
		
		this.cf = columnFamilyName;
		
	}
	static byte [] zeroByte = new byte[1];
	static byte [] byteBuffByte = new byte[1]; 
	static byte [] strByte = new byte[1];
	static byte [] byteArrByte = new byte[1];
	static byte [] longByte = new byte[1];
	static byte [] intByte = new byte[1];
	static byte [] shortByte = new byte[1];
	static byte [] compByte = new byte[1];
	static byte [] objByte = new byte[1];
	static byte [] bigIntByte = new byte[1];
	
	static Map<Class<?>,byte[]> clToByteMap = new HashMap<Class<?>,byte[]>();
	static {
		zeroByte[0] = new Integer(0).byteValue();
		clToByteMap.put(Class.class, zeroByte);
		
		strByte[0] = new Integer(1).byteValue();
		clToByteMap.put(String.class, strByte);
		
		byteArrByte[0] = new Integer(2).byteValue();
		clToByteMap.put(byte [].class, byteArrByte);
		
		longByte[0] = new Integer(3).byteValue();
		clToByteMap.put(Long.class, longByte);
		clToByteMap.put(long.class, longByte);
		
		intByte[0] = new Integer(4).byteValue(); 
		clToByteMap.put(Integer.class, intByte);
		clToByteMap.put(int.class, intByte);
		
		shortByte[0] = new Integer(5).byteValue();
		clToByteMap.put(Short.class, shortByte);
		clToByteMap.put(short.class, shortByte);
		
		
		byteBuffByte[0] = new Integer(6).byteValue();
		clToByteMap.put(ByteBuffer.class, byteBuffByte);
			
		
		objByte[0] = new Integer(7).byteValue();
		clToByteMap.put(Object.class, intByte);
				
		
		bigIntByte[0] = new Integer(8).byteValue();
		clToByteMap.put(BigInteger.class, bigIntByte);
		
		//byteArrByte[0] = new Integer(2).byteValue();
		
		
	}
	
	
	@Override
	public void insertIndex(N name, V value, K pkValue) throws ConnectionException {
		MutationBatch m = keyspace.prepareMutationBatch();
		
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		ColumnFamily<Composite, Composite> CF = new ColumnFamily<Composite, Composite>(
				"index_cf", compSerializer, compSerializer);
		
		Composite row = new Composite(this.cf,name,value);
				
		ByteBuffer buffer = TypeInferringSerializer.get().toByteBuffer(pkValue);
		Composite col = new Composite(getType(pkValue),buffer);
		
		//valueless
		m.withRow(CF, row).putColumn(col, zeroByte);
		
		m.execute();
		
	}

	
	@Override
	public void updateIndex(N name, V value, V oldValue, K pkValue)
			throws ConnectionException {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		ColumnFamily<Composite, Composite> CF = new ColumnFamily<Composite, Composite>(
				"index_cf", compSerializer, compSerializer);
		
		byte [] pkType = getType(pkValue);
		Composite row = new Composite(this.cf,name,value,pkType);
		Composite oldRow = new Composite(this.cf,name,pkType);
			
		ByteBuffer newBuffer = TypeInferringSerializer.get().toByteBuffer(pkValue);
		Composite newCol = new Composite(getType(pkValue),newBuffer);
		
		ByteBuffer oldValBuf = TypeInferringSerializer.get().toByteBuffer(oldValue);
		Composite oldCol = new Composite(getType(pkValue),oldValBuf);
		
		m.withRow(CF, oldRow).deleteColumn(oldCol);
		m.withRow(CF, row).putColumn(newCol, zeroByte);
		
		m.execute();
		
		
	}

	@Override
	public Collection<K> eq(N name, V value) throws ConnectionException {
		
		
		
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		ColumnFamily<Composite, Composite> CF = new ColumnFamily<Composite, Composite>(
				"index_cf", compSerializer, compSerializer);
		
		Composite row = new Composite(this.cf,name,value);
		
		
		OperationResult<ColumnList<Composite>> result = keyspace.prepareQuery(CF).getKey(row).execute();
		Collection<Composite> col = result.getResult().getColumnNames();
		ArrayList<K> list = new ArrayList<K>(col.size());
		
		//get serializer once
		byte []byteType = null;
		Serializer<K> ser = null;
		if (!col.isEmpty()) {
			byteType = (byte[])col.iterator().next().get(0);
			ser = getSerializer(byteType);
		}
		
		for (Composite comp:col) {
					
			byte []pkVal = (byte[])comp.get(1);
			list.add( ser.fromBytes(pkVal) );
		}
		
		
		return list;
		
	}

	public static <K>Serializer<K> getSerializer(byte []b) {
		Serializer<?> serializer = BytesArraySerializer.get();
		if (strByte.equals(b)) {
            serializer = StringSerializer.get();
        }
        else if (longByte.equals(b)) {
            serializer = LongSerializer.get();
        }
        else if (intByte.equals(b)) {
            serializer = IntegerSerializer.get();
        }
        else if (shortByte.equals(b)) {
            serializer = ShortSerializer.get();
        }
        /*else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
            serializer = BooleanSerializer.get();
        }*/
        else if (byteArrByte.equals(b)) {
            serializer = BytesArraySerializer.get();
        }
        else if (byteBuffByte.equals(b)) {
            serializer = ByteBufferSerializer.get();
        }
        else if (objByte.equals(b)) {
            serializer = ObjectSerializer.get();
        }
		
		return (Serializer<K>)serializer;
	}
	private static byte[] getType(Class<?> cl) {
		return clToByteMap.get(cl);
		
	}
	private static byte[] getType(Object value) {
		 
		byte [] ret = zeroByte;
	    ret = getType(value.getClass());   
	    return ret;
	}
	
	
	
}
