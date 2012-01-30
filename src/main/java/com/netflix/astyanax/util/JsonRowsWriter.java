package com.netflix.astyanax.util;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;

import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class JsonRowsWriter implements RowsWriter{
    public interface ErrorHandler {
        boolean onException(Exception e);
    }
    
    enum Field {
    	ROW_KEY,
    	ID,
    	COUNT,
    	NAMES,
    	ROWS,
    	COLUMN,
    	TIMESTAMP,
    	VALUE,
    }
    
    private Map<Field, String> fieldNames = new HashMap<Field, String>();
    
    private String TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private final PrintWriter out;
    private final SerializerPackage serializers;
    private String extra = null;
    private String errorValueText = "???";
    private boolean rowsAsArray = true;
    private boolean ignoreExceptions = true;
    private boolean ignoreUndefinedColumns = false;
    private Set<String> dynamicNames;
    private Set<String> fixedNames;
    private List<String> fixedNamesList;
    private Set<String> ignoreNames = new HashSet<String>();
    private Set<String> metadataNames;
    private ExceptionCallback exceptionCallback;
    private boolean columnsAsRows = false;
    private boolean idIsColumn = false;
    private int rowCount = 0;
    private int columnCount = 0;
    private int maxStringLength = 256;
    
    public JsonRowsWriter(PrintWriter out, SerializerPackage serializers) throws ConnectionException {
        this.out = out;
        this.serializers = serializers;
        
        fieldNames.put(Field.NAMES, "names");
        fieldNames.put(Field.ROWS, "rows");
        fieldNames.put(Field.COUNT, "count");
        fieldNames.put(Field.ID, "_id");
        fieldNames.put(Field.ROW_KEY, "key");
        fieldNames.put(Field.COLUMN, "column");
        fieldNames.put(Field.TIMESTAMP, "timestamp");
        fieldNames.put(Field.VALUE, "value");
    }
    
    public JsonRowsWriter setRowsName(String fieldName) {
        fieldNames.put(Field.ROWS, fieldName);
        return this;
    }
    
    public JsonRowsWriter setIdName(String fieldName) {
        fieldNames.put(Field.ID, fieldName);
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
    
    public JsonRowsWriter setErrorValueText(String text) {
        this.errorValueText = text;
        return this;
    }
    
    public JsonRowsWriter setDynamicColumnNames(boolean flag) {
        if (flag) {
            dynamicNames = new HashSet<String>();
        }
        else {
            dynamicNames = null;
        }
        return this;
    }
    
    public JsonRowsWriter setFixedColumnNames(String ... columns) {
        this.fixedNames = new HashSet<String>(Arrays.asList(columns));
        this.fixedNamesList = Arrays.asList(columns);
        return this;
    }
    
    public JsonRowsWriter setIgnoreColumnNames(String ... columns) {
        this.ignoreNames = new HashSet<String>(Arrays.asList(columns));
        return this;
    }

    public JsonRowsWriter setExceptionCallback(ExceptionCallback exceptionCallback) {
        this.exceptionCallback = exceptionCallback;
        return this;
    }
    
    public JsonRowsWriter setColumnsAsRows(boolean columnsAsRows) {
        this.columnsAsRows = columnsAsRows;
        setFixedColumnNames("column", "value", "timestamp");
        return this;
    }
    
    public JsonRowsWriter setIdIsColumn(boolean idIsColumn) {
        this.idIsColumn = idIsColumn;
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
            List<String> names = new ArrayList<String>(metadataNames);
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
                        out.append(jsonifyString(this.fieldNames.get(Field.ID))).append(":").append(jsonifyString(idString)).append(",");
                        out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY))).append(":").append(jsonifyString(idString));
                        writeColumns(row.getColumns());
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
                        out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY))).append(":").append(jsonifyString(idString));
                        writeColumns(row.getColumns());
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
            List<String> names = new ArrayList<String>(this.dynamicNames);
            Collections.sort(names);
            writeColumnNames(names);
        }
        
        if (extra != null) {
            out.append(extra).println(",");
        }
        out.append(jsonifyString(this.fieldNames.get(Field.COUNT))).append(":").append(Integer.toString(count)).println();
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
                    out.append(jsonifyString(this.fieldNames.get(Field.ID))).append(":").append(jsonifyString(rowKey + columnString));
                }
                else {
                    out.append(jsonifyString(rowKey + columnString)).append(":{");
                }
                out.print(",");
                
                String timestampString;
                try {
                    timestampString = new SimpleDateFormat(TIME_FORMAT_STRING).format(new Date(column.getTimestamp()/1000));
                }
                catch (Exception e) {
                    timestampString = "none";
                }
                     
                columnCount++;
                out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY))).append(":").append(jsonifyString(rowKey)).append(",")
                   .append(jsonifyString(this.fieldNames.get(Field.COLUMN))).append(":").append(jsonifyString(columnString)).append(",")
                   .append(jsonifyString(this.fieldNames.get(Field.VALUE))).append(":").append(jsonifyString(valueString)).append(",")
                   .append(jsonifyString(this.fieldNames.get(Field.TIMESTAMP))).append(":").append(jsonifyString(timestampString))
                   .append("}");
            }
            catch (Exception e) {
                if (!ignoreExceptions) {
                    throw e;
                }
            }
        }    
        
        return columns.size();
    }
    
    private void writeColumns(ColumnList<?> columns) throws Exception {
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
                    valueString = e.getMessage(); //  this.errorValueText;
                }
                
                out.append(",").append(jsonifyString(columnString)).append(":").append(jsonifyString(valueString));
            }
            catch (Exception e) {
                if (!ignoreExceptions) {
                    throw e;
                }
            }
        }
    }
    
    private Set<String> getColumnNamesFromMetadata() throws Exception {
        Set<String> set = new HashSet<String>();
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
            
            if (this.idIsColumn) {
                out.append(jsonifyString(this.fieldNames.get(Field.ID))).append(",");
            }

            out.append(jsonifyString(this.fieldNames.get(Field.ROW_KEY)));
            
            for (String name : names) {
                if (this.ignoreNames.contains(name)) {
                    continue;
                }
                try {
                    out.append(",");
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
    		return new StringBuilder().append("\"").append(JSONObject.escape(str.substring(0, maxStringLength) + "...")).append("\"").toString();
    	}
    	else {
    		return new StringBuilder().append("\"").append(JSONObject.escape(str)).append("\"").toString();
    	}
    }
}
