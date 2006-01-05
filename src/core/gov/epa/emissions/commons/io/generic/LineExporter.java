package gov.epa.emissions.commons.io.generic;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;

public class LineExporter extends GenericExporter {
    public LineExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        super(dataset, datasource, new LineFileFormat(sqlDataTypes));
    }

    public LineExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory) {
        super(dataset, datasource, new LineFileFormat(sqlDataTypes), formatFactory);
    }
    
    protected void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        String line = data.getString(cols[1].name());
        if(line.equalsIgnoreCase("-9"))
            writer.println();
        else
            writer.println(line);
    }

}
