package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class GridCrossReferenceExporter extends GenericExporter {
    
    public GridCrossReferenceExporter(Dataset dataset, String rowFilters, DbServer dbServer, Integer optimizedBatchSize) {
        super(dataset, rowFilters, dbServer, new GridCrossRefFileFormat(dbServer.getSqlDataTypes()), optimizedBatchSize);
    }
    
    public GridCrossReferenceExporter(Dataset dataset, String rowFilters, DbServer dbServer, 
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, rowFilters, dbServer, new GridCrossRefFileFormat(dbServer.getSqlDataTypes()), factory, optimizedBatchSize);
    }

}
