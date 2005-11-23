package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.speciation.SpeciationProfileExporter;

import java.io.File;

public class InventoryTableExporter {
    private SpeciationProfileExporter delegate;
 
    public InventoryTableExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        delegate = new SpeciationProfileExporter(dataset, datasource, fileFormat);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }
    
}
