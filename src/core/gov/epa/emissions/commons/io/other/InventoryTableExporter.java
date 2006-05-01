package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class InventoryTableExporter extends GenericExporter {  
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), optimizedBatchSize);
        setDelimiter("");
    }
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), factory, optimizedBatchSize);
        setDelimiter("");
    } 
}
