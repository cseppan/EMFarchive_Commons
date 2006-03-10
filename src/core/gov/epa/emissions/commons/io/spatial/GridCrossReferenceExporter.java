package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class GridCrossReferenceExporter extends GenericExporter {
    
    public GridCrossReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new GridCrossRefFileFormat(types));
    }
    
    public GridCrossReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new GridCrossRefFileFormat(types), factory);
    }

}
