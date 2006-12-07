package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SurrogatesDescriptionExporter extends GenericExporter {
    
    private long exportedLinesCount = 0;

    public SurrogatesDescriptionExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new SurrogatesDescriptionFileFormat(types), optimizedBatchSize);
    }
    
    public SurrogatesDescriptionExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new SurrogatesDescriptionFileFormat(types), factory, optimizedBatchSize);
    }
    
    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        for (int i = startCol(cols); i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            String towrite = getValue(cols, i, value, data);
            if(cols[i - 1].equalsIgnoreCase("NAME") && towrite.length() <= 10)
                towrite = "\"" + towrite + "\""; //if length > 10, quotes already added
            writer.write(towrite);

            if (i + 1 < cols.length)
                writer.print(getDelimiter());// delimiter
        }
        writer.println();
        ++exportedLinesCount;
    }
    
    public long getExportedLinesCount() {
        return this.exportedLinesCount;
    }
    
}
