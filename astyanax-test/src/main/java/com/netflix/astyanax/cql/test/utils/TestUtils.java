package com.netflix.astyanax.cql.test.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class TestUtils {

	public static class TestTokenRange {
		
		public String startToken;
		public String endToken;
		public List<String> expectedRowKeys = new ArrayList<String>();
		
		public TestTokenRange(String start, String end, String ... expectedKeys) {
			startToken = start;
			endToken = end;
			expectedRowKeys.addAll(Arrays.asList(expectedKeys));
		}
	}
	
	public static List<TestTokenRange> getTestTokenRanges() {
		
		/**
		 * HERE IS THE ACTUAL ORDER OF KEYS SORTED BY THEIR TOKENS	
		 * 
		 * -1671667184962092389   = Q,   -1884162317724288694   = O,   -2875471597373478633   = K, 
		 * -300136452241384611    = L,   -4197513287269367591   = H,   -422884050476930919    = Y, 
		 * -4837624800923759386   = B,   -4942250469937744623   = W,   -7139673851965614954   = J, 
		 * -7311855499978618814   = X,   -7912594386904524724   = M,   -8357705097978550118   = T, 
		 * -8692134701444027338   = C,    243126998722523514    = A,    3625209262381227179   = F, 
		 *  3846318681772828433   = R,    3914548583414697851   = N,    4834152074310082538   = I, 
		 *  4943864740760620945   = S,    576608558731393772    = V,    585625305377507626    = G, 
		 *  7170693507665539118   = E,    8086064298967168788   = Z,    83360928582194826     = P, 
		 *  8889191829175541774   = D,    9176724567785656400   = U
		 * 
		 */
		List<TestTokenRange> tokenRanges = new ArrayList<TestTokenRange>();
		
		tokenRanges.add(new TestTokenRange("-8692134701444027338", "-7912594386904524724","C", "T", "M"));
		tokenRanges.add(new TestTokenRange("-7311855499978618814", "-4942250469937744623","X", "J", "W"));
		tokenRanges.add(new TestTokenRange("-4837624800923759386", "-2875471597373478633","B", "H", "K"));
		tokenRanges.add(new TestTokenRange("-1884162317724288694", "-422884050476930919","O", "Q", "Y"));
		tokenRanges.add(new TestTokenRange("-300136452241384611", "243126998722523514","L", "P", "A"));
		tokenRanges.add(new TestTokenRange("576608558731393772", "3625209262381227179","V", "G", "F"));
		tokenRanges.add(new TestTokenRange("3846318681772828433", "4834152074310082538","R", "N", "I"));
		tokenRanges.add(new TestTokenRange("4943864740760620945", "8086064298967168788","S", "E", "Z"));
		tokenRanges.add(new TestTokenRange("8889191829175541774", "9176724567785656400","D", "U"));

		return tokenRanges;
	}

	/** CERTAIN COLUMN FAMILIES THAT GET RE-USED A LOT FOR DIFFERENT UNIT TESTS */
	
	public static ColumnFamily<String, String> CF_COLUMN_RANGE_TEST = ColumnFamily.newColumnFamily(
			"columnrange", // Column Family Name
			StringSerializer.get(), // Key Serializer
			StringSerializer.get(), // Column Serializer
			IntegerSerializer.get()); // Data serializer;
	
	public static void createColumnFamilyForColumnRange(Keyspace keyspace) throws Exception {
		keyspace.createColumnFamily(CF_COLUMN_RANGE_TEST, null);
	}
	
	public static void populateRowsForColumnRange(Keyspace keyspace) throws Exception {
		
        MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
        	String rowKey = Character.toString(keyName);
        	ColumnListMutation<String> colMutation = m.withRow(CF_COLUMN_RANGE_TEST, rowKey);
              for (char cName = 'a'; cName <= 'z'; cName++) {
            	  colMutation.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
              }
              m.withCaching(true);
              m.execute();
              m.discardMutations();
        }
	}

	public static void deleteRowsForColumnRange(Keyspace keyspace) throws Exception {
		
        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
            MutationBatch m = keyspace.prepareMutationBatch();
        	String rowKey = Character.toString(keyName);
        	m.withRow(CF_COLUMN_RANGE_TEST, rowKey).delete();
        	m.execute();
        	m.discardMutations();
        }
	}
}
