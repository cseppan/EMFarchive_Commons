package gov.epa.emissions.commons.io.other;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class InventoryTableExporter extends GenericExporter {
    
    private long exportedLinesCount = 0;
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), optimizedBatchSize);
        setDelimiter("");
    }
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), factory, optimizedBatchSize);
        setDelimiter("");
    } 
    
    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        int i = startCol(cols) + 1; //because of the extra Line_Number column
        for (; i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            writer.write(getValue(cols, i, value, data));

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
        writer.println();
        ++exportedLinesCount;
    }

    protected String formatValue(String[] cols, int index, ResultSet data) throws SQLException {
        int fileIndex = index;
        if (isTableVersioned(cols))
            fileIndex = index - 3;  //shifted by Dataset_Id, Version, Delete_Versions cols

        Column column = fileFormat.cols()[fileIndex - 3]; //shifted by obj_id, record_id, and Line_Number cols
        return (delimiter.equals("")) ? getFixedPositionValue(column, data) : getDelimitedValue(column, data);
    }
    
    public long getExportedLinesCount() {
        return this.exportedLinesCount;
    }
}
