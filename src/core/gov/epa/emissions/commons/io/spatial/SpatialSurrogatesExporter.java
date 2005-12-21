package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class SpatialSurrogatesExporter extends GenericExporter {
    public SpatialSurrogatesExporter(Dataset dataset, Datasource datasource, 
            SqlDataTypes types) {
        super(dataset, datasource, new SpatialSurrogatesFileFormat(types));
    }
}
