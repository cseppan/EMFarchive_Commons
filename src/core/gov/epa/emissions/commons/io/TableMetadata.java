package gov.epa.emissions.commons.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.io.ColumnMetaData;

public class TableMetadata {

    private String table;

    private List cols;

    public TableMetadata() {
        cols = new ArrayList();
    }

    public TableMetadata(String table) {
        this();
        this.table = table;
    }

    public void addColumnMetaData(ColumnMetaData col) {
        cols.add(col);
    }

    public ColumnMetaData[] getCols() {
        return (ColumnMetaData[]) cols.toArray(new ColumnMetaData[cols.size()]);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setCols(ColumnMetaData[] cols) {
        this.cols = Arrays.asList(cols);
    }

    public ColumnMetaData columnMetadata(String columnName) {
        for (int i = 0; i < cols.size(); i++) {
            ColumnMetaData metadata = (ColumnMetaData) cols.get(i);
            if (metadata.getName().equals(columnName)) {
                return metadata;
            }
        }
        return null;
    }

    public String[] getColNames() {
        ColumnMetaData[] cols = getCols();
        String[] colNames = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            colNames[i] = cols[i].getName();
        }

        return colNames;
    }

    public boolean containsCol(String col) {
        return cols.contains(col.toUpperCase()) || cols.contains(col.toLowerCase());
    }

}
