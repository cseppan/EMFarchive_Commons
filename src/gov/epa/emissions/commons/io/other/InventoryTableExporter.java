package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class InventoryTableExporter extends GenericExporter {
  
    public InventoryTableExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        super(dataset, datasource, fileFormat);
    }
    
}
