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
    
    private Dataset dataset;
    
    private String delimiter;
    
    public SurrogatesDescriptionExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new SurrogatesDescriptionFileFormat(types));
    }
    
    public SurrogatesDescriptionExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new SurrogatesDescriptionFileFormat(types), factory);
    }
    
    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        int i = startCol(cols) + 1;
        for (; i < cols.length + commentspad; i++) {
            if(data.getObject(i) != null) {
                String colValue = data.getObject(i).toString().trim();
                if(i == cols.length && !colValue.equals("")) {
                    if(colValue.charAt(0) == dataset.getInlineCommentChar())
                        writer.print(" " + colValue);
                    else
                        writer.print(" " + dataset.getInlineCommentChar() + colValue);
                } else {
                    if(cols[i - 1].equalsIgnoreCase("NAME"))
                        writer.print("\"" + colValue + "\"");
                    else
                        writer.print(colValue);
                }

                if (i + 1 < cols.length)
                    writer.print(delimiter);// delimiter
            }
        }
        writer.println();
    }
    
    public void setDelimiter(String del) {
        this.delimiter = del;
    }
}
