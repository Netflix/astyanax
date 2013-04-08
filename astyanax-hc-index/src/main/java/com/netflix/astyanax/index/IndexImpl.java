package com.netflix.astyanax.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.SerializerTypeInferer;
import com.netflix.astyanax.serializers.TypeInferringSerializer;


/**
 * Thread unsafe row based - high cardinality index 
 * 
 * The main idea is that "indexes" are stored in their own CF as
 * light weight composites with "pointers" back to the PK.
 * 
 * 
 * Can optionally store values as well, but not for this round.
 * 
 * Also thinking about changing the structure to not store in a single CF.
 * This will allow a lighter packaging in terms of what is shipped over
 * the wire, ie. less meta data about column family and also even the column 
 * name to be indexed.
 * Will complicate CF index naming, and possibly result in large CF names
 * which is possibly not good.
 * 
 * @author marcus
 *
 * @param <N> - the name of the column to be indexed
 * @param <V> - the value of the column to be compared (equals only)
 * @param <K> - the key to store as the reverse index
 */
/*
 drop column family index_cf;
 create column family index_cf with caching = 'ALL';
 */
public class IndexImpl<N,V,K> implements Index<N, V, K>
{

	Keyspace keyspace = null;
	//static AstyanaxContext<Keyspace> context = null;
	private MutationBatch mutationBatch;
	
	
	private String targetCF;
	public static String DEFAULT_INDEX_CF = "index_cf"; 
	private String indexCF = DEFAULT_INDEX_CF;
	
	/**
	 * Constructor required for reading
	 * @param keyspace
	 * @param targetCf
	 */
	
	protected IndexImpl(Keyspace keyspace,String targetCf,String indexCF) {
		this.keyspace = keyspace;
		this.targetCF = targetCf;
		this.indexCF = indexCF;
	}
	
	/**
	 * Will participate in this batch, using the default index CF
	 * 
	 * @param mutationBatch
	 */
	protected IndexImpl(MutationBatch mutationBatch,String targetCf) {
		this.mutationBatch = mutationBatch;
		this.targetCF = targetCf;
	}
	protected IndexImpl(MutationBatch mutationBatch,String targetCf,String indexCF) {
		this.mutationBatch = mutationBatch;
		this.targetCF = targetCf;
		this.indexCF = indexCF;
	}
	
	//support both read and write to index
	public IndexImpl(Keyspace keyspace,MutationBatch mutationBatch,String targetCf) {
		this.keyspace = keyspace;
		this.mutationBatch = mutationBatch;
		this.targetCF = targetCf;
	}
	
	public IndexImpl(Keyspace keyspace,MutationBatch mutationBatch,String targetCf,String indexCF) {
		this.keyspace = keyspace;
		this.mutationBatch = mutationBatch;
		this.targetCF = targetCf;
		this.indexCF = indexCF;
	}
	/**
	 * Same as above with create index column family optional.
	 * 
	 * @param keyspace
	 * @param mutationBatch
	 * @param targetCf
	 * @param indexCF
	 * @param create - creates the index column family if it doesn't exist
	 */
	public IndexImpl(Keyspace keyspace,MutationBatch mutationBatch,String targetCf,String indexCF,boolean create) throws ConnectionException {
		this(keyspace,mutationBatch,targetCf,indexCF);
		SchemaIndexUtil.createIndexCF(keyspace, indexCF, false, true);
	}
	
	
	/**
	 * 
	 * TODO: Check that paging is actually used
	 * ORs 
	 * use a paging mechanism and/or {@link AllRowsReader} recipe
	 * to retrieve and build the index.
	 * and own it's own mutator.
	 * 
	 */
	@Override
	public void buildIndex(String cf,N colName,Class<K> keyType) throws ConnectionException {
		
		
				
		Serializer<K> keySerializer = SerializerTypeInferer.getSerializer(keyType);
		Serializer<N> colSerializer = SerializerTypeInferer.getSerializer(colName.getClass());
		 
		ColumnFamily<K, N> colFamily = new ColumnFamily<K, N>(cf,keySerializer, colSerializer);
		
		//Get all rows: 
		MutationBatch m = mutationBatch;
		OperationResult<Rows<K,N>> result = keyspace.prepareQuery(colFamily)
				.getAllRows()
				.withColumnSlice(colName)
				.execute();
		Rows<K,N> rows = result.getResult();
		Iterator<Row<K,N>> iter =  rows.iterator();
		
		//The index column family
		CompositeSerializer compSerializer = CompositeSerializer.get();
		ColumnFamily<Composite, ByteBuffer> CF = new ColumnFamily<Composite, ByteBuffer>(
				indexCF, compSerializer, ByteBufferSerializer.get());
		//byte []pkType = getType(value)
		//put them all in: no updates
		while (iter.hasNext()) {
			Row<K,N> row = iter.next();
			
			//the value of the column
			byte []value = row.getColumns().getColumnByName(colName).getByteArrayValue();
						
			
			Composite toInsert = new Composite(this.targetCF,colName,value);
			
			ByteBuffer buffer = TypeInferringSerializer.get().toByteBuffer(row.getKey());
			
			m.withRow(CF, toInsert).putColumn(buffer, MappingUtil.getType(row.getKey()));
			
			
			
			
		}
		//m.execute();
		
		
	}
	
	

	@Override
	public void deleteIndex(String cf, N name) throws ConnectionException {
		// TODO Auto-generated method stub
		
	}

	//@Override
	public void createIndex(String columnFamilyName) {
		
		this.targetCF = columnFamilyName;
		
	}
	
	
	
	@Override
	public void insertIndex(N name, V value, K pkValue)  {
		
		
		
		
		Composite row = new Composite(this.targetCF);
		if (name instanceof Composite  ) 		
			row.add(CompositeSerializer.get().toBytes((Composite)name));
		else
			row.add(name);
		if (value instanceof Composite)
			row.add(CompositeSerializer.get().toBytes((Composite)value));
		else
			row.add(value);
		
		
				
		byte []bType = MappingUtil.getType(pkValue);
		
		
		//I suppose that if we can save space by
		//storing the type information else-where
		if (pkValue instanceof Composite) {
			byte [] compositeColNameByte = CompositeSerializer.get().toBytes((Composite)pkValue);
			ColumnFamily<Composite, byte[]> CF = new ColumnFamily<Composite, byte[]>(
					indexCF,CompositeSerializer.get(), BytesArraySerializer.get());
			
			mutationBatch.withRow(CF, row).putColumn(compositeColNameByte, bType);
			//mutationBatch.withRow(CF, CompositeSerializer.get().toBytes(row));//.putColumn(pkValue, bType);
		}else {
			mutationBatch.withRow(getRowCF(), row).putColumn(pkValue,bType);
		}
		
		
		
	}
	
	private ColumnFamily<Composite,K> getRowCF() {
		CompositeSerializer compSerializer = CompositeSerializer.get();
		Serializer<K> colSerizalier = TypeInferringSerializer.get();
		
		ColumnFamily<Composite, K> CF = new ColumnFamily<Composite, K>(
				indexCF, compSerializer, colSerizalier);
		return CF;
	}
	
	@Override
	public void updateIndex(N name, V value, V oldValue, K pkValue)
			 {
		
		insertIndex(name, value, pkValue);
		
		//remove the old row
		removeIndex(name,oldValue,pkValue);
		
		
		
	}
	

	@Override
	public void removeIndex(N name, V value, K pkValue) {
		Composite row = new Composite(this.targetCF,name,value);
		mutationBatch.withRow(getRowCF(), row).deleteColumn(pkValue);
	}

	@Override
	public Collection<K> eq(N name, V value) throws ConnectionException {
		
				
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		BytesArraySerializer bSer = BytesArraySerializer.get();
		
		ColumnFamily<Composite, byte[]> CF = new ColumnFamily<Composite, byte[]>(
				indexCF, compSerializer, bSer);
		
		Composite row = new Composite(this.targetCF);
		if (name instanceof Composite  ) 		
			row.add(CompositeSerializer.get().toBytes((Composite)name));
		else
			row.add(name);
		if (value instanceof Composite)
			row.add(CompositeSerializer.get().toBytes((Composite)value));
		else
			row.add(value);
		
		
		OperationResult<ColumnList<byte[]>> result = keyspace.prepareQuery(CF).getKey(row).execute();
		
		if (result.getResult().isEmpty()) {
			return new ArrayList<K>(0);
		}
		
		Collection<byte[]> col = result.getResult().getColumnNames();
		ArrayList<K> list = new ArrayList<K>(col.size());
		
		
		//get serializer once
		byte []byteType = result.getResult().getColumnByIndex(0).getByteArrayValue();
		Serializer<K> ser = MappingUtil.getSerializer(byteType);
		
						
		
		for (byte[] comp:col) {						
			
			K val = ser.fromBytes(comp);
			//byte []pkVal = ((ByteBuffer)comp.get(1)).array();
			list.add( val );
		}
		
		
		return list;
		
	}

	/**
	 * A bit of code redundancy here, as it's a copy (almost) from above.
	 * 
	 */
	@Override
	public Collection<byte[]> eqBytes(N name, V value)
			throws ConnectionException {
		CompositeSerializer compSerializer = CompositeSerializer.get();
		
		BytesArraySerializer bSer = BytesArraySerializer.get();
		
		ColumnFamily<Composite, byte[]> CF = new ColumnFamily<Composite, byte[]>(
				targetCF, compSerializer, bSer);
		
		Composite row = new Composite(this.targetCF,name,value);
		
		
		OperationResult<ColumnList<byte[]>> result = keyspace.prepareQuery(CF).getKey(row).execute();
		
		if (result.getResult().isEmpty()) {
			return new ArrayList<byte[]>(0);
		}
		
		Collection<byte[]> col = result.getResult().getColumnNames();
		ArrayList<byte[]> list = new ArrayList<byte[]>(col.size());
		
					
		
		for (byte[] comp:col) {						
			
			list.add( comp );
		}
		
		
		return list;
	}
	
	
	
	
	
}
