package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LineExporter extends GenericExporter {

    public LineExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizedBatchSize) {
        super(dataset, dbServer, new LineFileFormat(sqlDataTypes), optimizedBatchSize);
    }

    public LineExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, DataFormatFactory formatFactory,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, new LineFileFormat(sqlDataTypes), formatFactory, optimizedBatchSize);
    }

    protected void writeDataCols(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        writer.write(data.getString(startColNumber));
    }

    protected int startCol(String[] cols) {
        if (isTableVersioned(cols))
            return 6; // shifted by "Obj_Id", "Record_Id",
        // "Dataset_Id", "Version", "Delete_Versions", "Line_Number"

        return 3; // shifted by "Obj_Id", "Record_Id", "Line_Number"
    }

}
