package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LineExporter extends GenericExporter {
    private Dataset dataset;
    
    public LineExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        super(dataset, dbServer, new LineFileFormat(sqlDataTypes));
        this.dataset = dataset;
    }

    public LineExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory) {
        super(dataset, dbServer, new LineFileFormat(sqlDataTypes), formatFactory);
        this.dataset = dataset;
    }
    
    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        int i = startCol(cols) + 2;
        if(data.getObject(i) != null) {
            String line = data.getObject(i).toString().trim();
            String comment = (String)data.getObject(i + commentspad);
            if(comment != null)
                comment = comment.trim();
            else 
                comment = "";
            
            if(line.equals("-9"))
                line = "";
            
            writer.print(line);
            
            if(commentspad == 1 && !comment.equals("")) {
                if(comment.charAt(0) == dataset.getInlineCommentChar())
                    writer.print(" " + comment);
                else
                    writer.print(" " + dataset.getInlineCommentChar() + comment);
            }
        }
       
        writer.println();
    }

}
