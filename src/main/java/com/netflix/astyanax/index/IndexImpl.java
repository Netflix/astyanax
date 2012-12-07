package com.netflix.astyanax.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

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
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;
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
	
	/**
	 * Will participate in this batch.
	 * 
	 * @param mutationBatch
	 */
	public IndexImpl(MutationBatch mutationBatch) {
		this.mutationBatch = mutationBatch;
	}
	
	//TODO
	//this should be moved out.
	//In the case that keyspace is not provided, ie, on read path then
	//we could optionally construct it.
	//this would mean that the client would need to have to provide this
	//or at least some configuration for it
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
	
	private MutationBatch getMutation() {
		if (mutationBatch == null)
			init();
		return mutationBatch;
			
	}
	
	/**
	 * TODO: use a paging mechanism and/or {@link AllRowsReader} recipe
	 * to retrieve and build the index.
	 * 
	 */
	@Override
	public void buildIndex(String cf,N colName,Class<K> keyType) throws ConnectionException {
		
		
				
		Serializer<K> keySerializer = SerializerTypeInferer.getSerializer(keyType);
		Serializer<N> colSerializer = SerializerTypeInferer.getSerializer(colName.getClass());
		 
		ColumnFamily<K, N> colFamily = new ColumnFamily<K, N>(cf,keySerializer, colSerializer);
		
		//Get all rows: 
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
			
			m.withRow(CF, toInsert).putColumn(buffer, MappingUtil.getType(row.getKey()));
			
			
			
			
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
	
	
	
	@Override
	public void insertIndex(N name, V value, K pkValue) throws ConnectionException {
		MutationBatch m = keyspace.prepareMutationBatch();
		
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		ColumnFamily<Composite, Composite> CF = new ColumnFamily<Composite, Composite>(
				"index_cf", compSerializer, compSerializer);
		
		Composite row = new Composite(this.cf,name,value);
				
		ByteBuffer buffer = TypeInferringSerializer.get().toByteBuffer(pkValue);
		Composite col = new Composite(MappingUtil.getType(pkValue),buffer);
		
		//valueless
		m.withRow(CF, row).putEmptyColumn(col);
		
		m.execute();
		
	}

	
	@Override
	public void updateIndex(N name, V value, V oldValue, K pkValue)
			throws ConnectionException {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		ColumnFamily<Composite, Composite> CF = new ColumnFamily<Composite, Composite>(
				"index_cf", compSerializer, compSerializer);
		
		byte [] pkType = MappingUtil.getType(pkValue);
		Composite row = new Composite(this.cf,name,value,pkType);
		Composite oldRow = new Composite(this.cf,name,pkType);
			
		ByteBuffer newBuffer = TypeInferringSerializer.get().toByteBuffer(pkValue);
		Composite newCol = new Composite(MappingUtil.getType(pkValue),newBuffer);
		
		ByteBuffer oldValBuf = TypeInferringSerializer.get().toByteBuffer(oldValue);
		Composite oldCol = new Composite(MappingUtil.getType(pkValue),oldValBuf);
		
		m.withRow(CF, oldRow).deleteColumn(oldCol);
		m.withRow(CF, row).putEmptyColumn(newCol);
		
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
			ser = MappingUtil.getSerializer(byteType);
		}
		
		for (Composite comp:col) {
					
			byte []pkVal = (byte[])comp.get(1);
			list.add( ser.fromBytes(pkVal) );
		}
		
		
		return list;
		
	}

	
	
	
	
}
