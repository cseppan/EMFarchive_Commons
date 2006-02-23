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

}
