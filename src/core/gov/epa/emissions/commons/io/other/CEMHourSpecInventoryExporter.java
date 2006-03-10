package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;

public class CEMHourSpecInventoryExporter extends GenericExporter {
    public CEMHourSpecInventoryExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new CEMHourSpecInventFileFormat(types));
        setup();
    }
    
    public CEMHourSpecInventoryExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new CEMHourSpecInventFileFormat(types), factory);
        setup();
    }
    
    private void setup(){
        this.setDelimiter(",");
    }
    
    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        writer.print(dataset.getDescription());
    }
       
}
