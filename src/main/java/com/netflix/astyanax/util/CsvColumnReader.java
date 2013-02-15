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
        columns.add(Pair.create("key", row[0]));
        if (row.length == 3)
            columns.add(Pair.create(row[1], row[2]));
        return columns;
    }
}
