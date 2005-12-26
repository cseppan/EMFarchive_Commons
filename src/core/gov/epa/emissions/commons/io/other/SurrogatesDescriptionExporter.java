package gov.epa.emissions.commons.io.other;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class SurrogatesDescriptionExporter extends GenericExporter {
    public SurrogatesDescriptionExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {  
        super(dataset, datasource, new SurrogatesDescriptionFileFormat(types));
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