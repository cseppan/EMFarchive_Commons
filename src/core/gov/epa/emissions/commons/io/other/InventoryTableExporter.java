package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class InventoryTableExporter extends GenericExporter {  
    public InventoryTableExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        super(dataset, datasource, new InventoryTableFileFormat(types, 1));
    }
    
    public InventoryTableExporter(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, datasource, new InventoryTableFileFormat(types, 1), factory);
    } 
}
