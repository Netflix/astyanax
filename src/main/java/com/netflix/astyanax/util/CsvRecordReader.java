package com.netflix.astyanax.util;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.csv.CSVParser;

/**
 * 
 * @author elandau
 *
 */
public class CsvRecordReader implements RecordReader {
	private CSVParser parser;
	private boolean hasHeaderLine = true;
	private String[] names = null;
	
	public CsvRecordReader(Reader reader) {
		this.parser = new CSVParser(reader);
	}

	public CsvRecordReader setHasHeaderLine(boolean flag) {
		this.hasHeaderLine = flag;
		return this;
	}
	
    public CsvRecordReader setNames(String ... names) {
        this.names = names;
        return this;
    }
	@Override
	public void start() throws IOException {
		// First line contains the column names.  First column expected to be the row key
		if (hasHeaderLine) {
			names = parser.getLine();
		}
	}
	
	@Override
	public void shutdown() {
		
	}
	
	@Override
	public List<Pair<String, String>> next() throws IOException {
		// Iterate rows
		String[] row = parser.getLine();
		if (null == row) 
			return null;
		
		List<Pair<String, String>> columns = new ArrayList<Pair<String, String>>();
		
		// Build row mutation for all columns
		for (int i = 0; i < row.length; i++) {
		    if (i >= names.length) {
		        // Ignore past size of names
		        break;
		    }
			columns.add(new Pair<String, String>(names[i], row[i]));
		}
		return columns;
	}
}
