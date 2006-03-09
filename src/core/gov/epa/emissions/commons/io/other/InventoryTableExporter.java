package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class InventoryTableExporter extends GenericExporter {  
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1));
        setDelimiter("");
    }
    
    public InventoryTableExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new InventoryTableFileFormat(types, 1), factory);
        setDelimiter("");
    } 
}
