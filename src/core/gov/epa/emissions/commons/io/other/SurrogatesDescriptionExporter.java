package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
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
    
    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        for (int i = startCol(cols); i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            String towrite = getValue(cols, i, value, data);
            if(cols[i - 1].equalsIgnoreCase("NAME"))
                towrite = "\"" + towrite + "\"";
            writer.write(towrite);

            if (i + 1 < cols.length)
                writer.print(getDelimiter());// delimiter
        }
        writer.println();
    }
    
}
