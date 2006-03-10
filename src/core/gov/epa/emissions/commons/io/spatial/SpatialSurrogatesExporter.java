package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class SpatialSurrogatesExporter extends GenericExporter {
    public SpatialSurrogatesExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new SpatialSurrogatesFileFormat(types));
    }
    
    public SpatialSurrogatesExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new SpatialSurrogatesFileFormat(types), factory);
    }

}
