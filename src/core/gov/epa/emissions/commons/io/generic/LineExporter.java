package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LineExporter extends GenericExporter {
    public LineExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        super(dataset, dbServer, new LineFileFormat(sqlDataTypes));
    }

    public LineExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, DataFormatFactory formatFactory) {
        super(dataset, dbServer, new LineFileFormat(sqlDataTypes), formatFactory);
    }
    
    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        for (int i = startCol(cols) + 1; i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            if (value != null)
                writer.write(getValue(cols, i, value, data));
        }
        writer.println();
    }

}
