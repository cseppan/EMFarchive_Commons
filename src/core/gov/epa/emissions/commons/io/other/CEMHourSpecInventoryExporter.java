package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;

public class CEMHourSpecInventoryExporter extends GenericExporter {
    public CEMHourSpecInventoryExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        super(dataset, datasource, new CEMHourSpecInventFileFormat(types));
        setup();
    }
    
    public CEMHourSpecInventoryExporter(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, datasource, new CEMHourSpecInventFileFormat(types), factory);
        setup();
    }
    
    private void setup(){
        this.setDelimiter(",");
        this.setFormatted(false);
    }
    
    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        writer.print(dataset.getDescription());
    }
       
}
