package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class PointStackReplacementsExporter extends GenericExporter {
    
    public PointStackReplacementsExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new PointStackReplacementsFileFormat(types), optimizedBatchSize);
    }
    
    public PointStackReplacementsExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new PointStackReplacementsFileFormat(types), factory, optimizedBatchSize);
    }
}
