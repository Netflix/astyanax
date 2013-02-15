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

    public CsvRecordReader setNames(String... names) {
        this.names = names;
        return this;
    }

    @Override
    public void start() throws IOException {
        // First line contains the column names. First column expected to be the
        // row key
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
            columns.add(Pair.create(names[i], row[i]));
        }
        return columns;
    }
}
