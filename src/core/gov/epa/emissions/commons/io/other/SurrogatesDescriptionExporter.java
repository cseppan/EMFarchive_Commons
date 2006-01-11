package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SurrogatesDescriptionExporter extends GenericExporter {
    
    public SurrogatesDescriptionExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new SurrogatesDescriptionFileFormat(types));
    }
    
    public SurrogatesDescriptionExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new SurrogatesDescriptionFileFormat(types), factory);
    }
    
    protected void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            if(cols[i].name().equalsIgnoreCase("NAME"))
                writer.print("\"" + cols[i].format(data).trim() + "\"");
            else
                writer.print(cols[i].format(data).trim());
            
            if (i + 1 < cols.length)
                writer.print(",");// delimiter
        }
        writer.println();
    }
}
