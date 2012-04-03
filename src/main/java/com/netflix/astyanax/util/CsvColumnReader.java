package com.netflix.astyanax.util;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.csv.CSVParser;

/**
 * Read a CSV where each row represents a single column
 * 
 * rowkey, columname, columnvalue
 * 
 * @author elandau
 * 
 */
public class CsvColumnReader implements RecordReader {
    private CSVParser parser;
    private boolean hasHeaderLine = true;

    public CsvColumnReader(Reader reader) {
        this.parser = new CSVParser(reader);
    }

    public CsvColumnReader setHasHeaderLine(boolean flag) {
        this.hasHeaderLine = flag;
        return this;
    }

    @Override
    public void start() throws IOException {
        // First line contains the column names. First column expected to be the
        // row key
        if (hasHeaderLine) {
            parser.getLine();
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
        columns.add(new Pair<String, String>("key", row[0]));
        if (row.length == 3)
            columns.add(new Pair<String, String>(row[1], row[2]));
        return columns;
    }
}
