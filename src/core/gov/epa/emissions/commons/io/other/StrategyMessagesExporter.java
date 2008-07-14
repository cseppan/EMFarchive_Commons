package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;
import java.sql.SQLException;

public class StrategyMessagesExporter extends GenericExporter {
    public StrategyMessagesExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new StrategyMessagesFileFormat(types), optimizedBatchSize);
        setup();
    }
    
    public StrategyMessagesExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new StrategyMessagesFileFormat(types), factory, optimizedBatchSize);
        setup();
    }
    
    private void setup(){
        this.setDelimiter(",");
    }
    
    protected void writeHeaders(PrintWriter writer, Dataset dataset) throws SQLException {
        writer.print(dataset.getDescription());
        printExportInfo(writer);
    }
       
}
