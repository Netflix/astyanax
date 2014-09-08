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

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class JsonRowsWriter implements RowsWriter {
    public interface ErrorHandler {
        boolean onException(Exception e);
    }

    enum Field {
        ROW_KEY, COUNT, NAMES, ROWS, COLUMN, TIMESTAMP, VALUE, TTL
    }

    private Map<Field, String> fieldNames = Maps.newHashMap();

    private static final String TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    
    private final PrintWriter out;
    private final SerializerPackage serializers;
    private String extra = null;
    private boolean rowsAsArray = true;
    private boolean ignoreExceptions = true;
    private boolean ignoreUndefinedColumns = false;
    private Set<String> dynamicNames;
    private Set<String> fixedNames;
    private List<String> fixedNamesList;
    private Set<String> ignoreNames = Sets.newHashSet();
    private Set<String> metadataNames;
    private boolean columnsAsRows = false;
    private int rowCount = 0;
    private int columnCount = 0;
    private int maxStringLength = 256;
    private String rowColumnDelimiter = "$";

    public JsonRowsWriter(PrintWriter out, SerializerPackage serializers) throws ConnectionException {
        this.out = out;
        this.serializers = serializers;

        fieldNames.put(Field.NAMES,     "names");
        fieldNames.put(Field.ROWS,      "rows");
        fieldNames.put(Field.COUNT,     "count");
        fieldNames.put(Field.ROW_KEY,   "_key");
        fieldNames.put(Field.COLUMN,    "column");
        fieldNames.put(Field.TIMESTAMP, "timestamp");
        fieldNames.put(Field.VALUE,     "value");
        fieldNames.put(Field.TTL,       "ttl");
    }

    public JsonRowsWriter setRowsName(String fieldName) {
        fieldNames.put(Field.ROWS, fieldName);
        return this;
    }

    public JsonRowsWriter setNamesName(String fieldName) {
        fieldNames.put(Field.NAMES, fieldName);
        return this;
    }

    public JsonRowsWriter setCountName(String fieldName) {
        fieldNames.put(Field.COUNT, fieldName);
        return this;
    }

    public JsonRowsWriter setRowKeyName(String fieldName) {
        fieldNames.put(Field.ROW_KEY, fieldName);
        return this;
    }

    public JsonRowsWriter setExtra(String extra) {
        this.extra = extra;
        return this;
    }

    public JsonRowsWriter setRowsAsArray(boolean flag) {
        this.rowsAsArray = flag;
        return this;
    }

    public JsonRowsWriter setIgnoreUndefinedColumns(boolean flag) {
        this.ignoreUndefinedColumns = flag;
        return this;
    }

    @Deprecated
    public JsonRowsWriter setErrorValueText(String text) {
        return this;
    }

    public JsonRowsWriter setDynamicColumnNames(boolean flag) {
        if (flag) {
            dynamicNames = Sets.newLinkedHashSet();
        }
        else {
            dynamicNames = null;
        }
        return this;
    }

    public JsonRowsWriter setFixedColumnNames(String... columns) {
        this.fixedNames = Sets.newLinkedHashSet(Arrays.asList(columns));
        this.fixedNamesList = Arrays.asList(columns);
        return this;
    }

    public JsonRowsWriter setIgnoreColumnNames(String... columns) {
        this.ignoreNames = Sets.newHashSet(Arrays.asList(columns));
        return this;
    }

    @Deprecated
    public JsonRowsWriter setExceptionCallback(ExceptionCallback exceptionCallback) {
        return this;
    }

    public JsonRowsWriter setColumnsAsRows(boolean columnsAsRows) {
        this.columnsAsRows = columnsAsRows;
        setFixedColumnNames("column", "value", "timestamp", "ttl");
        return this;
    }

    public JsonRowsWriter addExtra(String name, String value) {
        if (extra == null) {
            extra = new String();
        }

        if (!extra.isEmpty()) {
            extra += ",";
        }
        extra += jsonifyString(name) + ":" + jsonifyString(value);
        return this;
    }

    public JsonRowsWriter setMaxLength(int maxStringLength) {
        this.maxStringLength = maxStringLength;
        return this;
    }

    int getColumnCount() {
        return this.columnCount;
    }

    int getRowCount() {
        return this.rowCount;
    }

    @Override
    public void write(Rows<?, ?> rows) throws Exception {
        this.rowCount = 0;
        this.columnCount = 0;

        out.println("{");

        if (this.fixedNamesList != null) {
            writeColumnNames(this.fixedNamesList);
        }
        else if (this.dynamicNames == null) {
            metadataNames = this.getColumnNamesFromMetadata();
            List<String> names = Lists.newArrayList(metadataNames);
            Collections.sort(names);
            writeColumnNames(names);
        }

        out.append(jsonifyString(this.fieldNames.get(Field.ROWS))).append(":");

        int count = 0;
        if (columnsAsRows) {
            addExtra("columnsAsRows", "true");
            if (rowsAsArray) {
                out.append("[");
            }
            boolean firstRow = true;
            for (Row<?, ?> row : rows) {
                if (row.getColumns().isEmpty())
                    continue;
                rowCount++;
                String idString = serializers.keyAsString(row.getRawKey());
                count += writeColumnsAsRows(idString, row.getColumns(), firstRow);
                firstRow = false;
            }

            if (rowsAsArray) {
                out.append("]");
            }
            out.println();
        }
        else {
            if (rowsAsArray) {
                out.append("[").println();

                boolean firstRow = true;
                for (Row<?, ?> row : rows) {
                    if (row.getColumns().isEmpty())
                        continue;
                    rowCount++;
                    if (!firstRow) {
                        out.println(",");
                    }
                    else {
                        firstRow = false;
                    }

                    out.append("{");
                    try {
                        String idString = serializers.keyAsString(row.getRawKey());
                        out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY))).append(":")
                                .append(jsonifyString(idString));
                        writeColumns(row.getColumns(), false);
                        count++;
                    }
                    catch (Exception e) {
                        if (!ignoreExceptions) {
                            throw e;
                        }
                    }
                    out.print("}");
                }

                out.println();
                out.append("]");
            }
            else {
                out.append("{").println();

                boolean firstRow = true;
                for (Row<?, ?> row : rows) {
                    if (row.getColumns().isEmpty())
                        continue;
                    rowCount++;
                    if (!firstRow) {
                        out.println(",");
                    }
                    else {
                        firstRow = false;
                    }

                    try {
                        String idString = serializers.keyAsString(row.getRawKey());
                        out.append(jsonifyString(idString)).append(":{");
                        // out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY))).append(":").append(jsonifyString(idString));
                        writeColumns(row.getColumns(), true);
                        out.print("}");
                        count++;
                    }
                    catch (Exception e) {
                        if (!ignoreExceptions) {
                            throw e;
                        }
                    }
                }
                out.println();
                out.append("}");
            }
        }
        out.println(",");

        if (this.dynamicNames != null) {
            List<String> names = Lists.newArrayList(this.dynamicNames);
            Collections.sort(names);
            writeColumnNames(names);
        }

        if (extra != null) {
            out.append(extra).println(",");
        }
        out.append(jsonifyString(this.fieldNames.get(Field.COUNT))).append(":").append(Integer.toString(count))
                .println();
        out.println("}");
    }

    private int writeColumnsAsRows(String rowKey, ColumnList<?> columns, boolean first) throws Exception {
        for (Column<?> column : columns) {
            try {
                String columnString;
                try {
                    columnString = serializers.columnAsString(column.getRawName());
                }
                catch (Exception e) {
                    e.printStackTrace();
                    if (!ignoreExceptions) {
                        throw e;
                    }
                    columnString = e.getMessage(); // this.errorValueText;
                }

                String valueString = null;
                try {
                    valueString = serializers.valueAsString(column.getRawName(), column.getByteBufferValue());
                }
                catch (Exception e) {
                    e.printStackTrace();
                    if (!ignoreExceptions) {
                        throw e;
                    }
                    valueString = e.getMessage(); // this.errorValueText;
                }

                if (!first) {
                    out.println(",");
                }
                else {
                    first = false;
                }
                out.append("{");
                if (rowsAsArray) {
                    out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY))).append(":")
                            .append(jsonifyString(rowKey));
                }
                else {
                    out.append(jsonifyString(rowKey + rowColumnDelimiter + columnString)).append(":{");
                }
                out.print(",");

                String timestampString;
                try {
                    timestampString = new SimpleDateFormat(TIME_FORMAT_STRING).format(new Date(
                            column.getTimestamp() / 1000));
                }
                catch (Exception e) {
                    timestampString = "none";
                }

                int ttl;
                try {
                    ttl = column.getTtl();
                }
                catch (Exception e) {
                    ttl = 0;
                }
                
                columnCount++;
                out.append(jsonifyString(this.fieldNames.get(Field.COLUMN))).append(":")
                        .append(jsonifyString(columnString)).append(",")
                        .append(jsonifyString(this.fieldNames.get(Field.VALUE))).append(":")
                        .append(jsonifyString(valueString)).append(",")
                        .append(jsonifyString(this.fieldNames.get(Field.TIMESTAMP))).append(":")
                        .append(jsonifyString(timestampString)).append(",")
                        .append(jsonifyString(this.fieldNames.get(Field.TTL))).append(":")
                        .append(jsonifyString(Integer.toString(ttl))).append("}")
                        ;
            }
            catch (Exception e) {
                if (!ignoreExceptions) {
                    throw e;
                }
            }
        }

        return columns.size();
    }

    private void writeColumns(ColumnList<?> columns, boolean first) throws Exception {
        for (Column<?> column : columns) {
            try {
                String columnString;
                try {
                    columnString = serializers.columnAsString(column.getRawName());
                }
                catch (Exception e) {
                    e.printStackTrace();
                    if (!ignoreExceptions) {
                        throw e;
                    }
                    columnString = e.getMessage(); // this.errorValueText;
                }
                if (this.ignoreNames.contains(columnString)) {
                    continue;
                }

                if (this.ignoreUndefinedColumns) {
                    if (this.fixedNames != null && !this.fixedNames.contains(columnString))
                        continue;
                    if (this.metadataNames != null && !this.metadataNames.contains(columnString))
                        continue;
                }

                if (this.dynamicNames != null)
                    this.dynamicNames.add(columnString);

                String valueString = null;
                try {
                    valueString = serializers.valueAsString(column.getRawName(), column.getByteBufferValue());
                }
                catch (Exception e) {
                    e.printStackTrace();
                    if (!ignoreExceptions) {
                        throw e;
                    }
                    valueString = e.getMessage(); // this.errorValueText;
                }

                if (!first)
                    out.append(",");
                else
                    first = false;
                out.append(jsonifyString(columnString)).append(":").append(jsonifyString(valueString));
            }
            catch (Exception e) {
                if (!ignoreExceptions) {
                    throw e;
                }
            }
        }
    }

    private Set<String> getColumnNamesFromMetadata() throws Exception {
        Set<String> set = Sets.newLinkedHashSet();
        try {
            for (ByteBuffer name : this.serializers.getColumnNames()) {
                try {
                    String columnName = this.serializers.columnAsString(name);
                    set.add(columnName);
                }
                catch (Exception e) {
                    if (!ignoreExceptions) {
                        throw e;
                    }
                }
            }
        }
        catch (Exception e) {
            if (!ignoreExceptions) {
                throw e;
            }
        }
        return set;
    }

    private void writeColumnNames(List<String> names) throws Exception {
        try {
            out.append(jsonifyString(this.fieldNames.get(Field.NAMES))).append(":[");

            boolean first = true;
            if (this.rowsAsArray) {
                out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY)));
                first = false;
            }

            for (String name : names) {
                if (this.ignoreNames.contains(name)) {
                    continue;
                }
                try {
                    if (!first)
                        out.append(",");
                    else
                        first = false;
                    out.append(jsonifyString(name));
                }
                catch (Exception e) {
                }
            }
            out.println("],");
        }
        catch (Exception e) {
            if (!ignoreExceptions) {
                throw e;
            }
        }
    }

    private String jsonifyString(String str) {
        if (str == null) {
            str = "null";
        }
        if (str.length() > maxStringLength) {
            return JSONObject.quote(str.substring(0, maxStringLength) + "...");
        }
        else {
            return JSONObject.quote(str);
        }
    }
}
