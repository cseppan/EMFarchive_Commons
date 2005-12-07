package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class AreaTemporalReferenceExporter extends GenericExporter {
    public AreaTemporalReferenceExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        super(dataset, datasource, fileFormat);
    }
}
