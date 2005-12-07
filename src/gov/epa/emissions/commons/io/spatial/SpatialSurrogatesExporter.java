package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.speciation.SpeciationProfileExporter;

public class SpatialSurrogatesExporter extends SpeciationProfileExporter {
    public SpatialSurrogatesExporter(Dataset dataset, Datasource datasource, 
            FileFormat fileFormat) {
        super(dataset, datasource, fileFormat);
    }
}
