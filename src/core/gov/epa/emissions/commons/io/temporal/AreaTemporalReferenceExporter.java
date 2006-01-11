package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class AreaTemporalReferenceExporter extends GenericExporter {
    
    public AreaTemporalReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new AreaTemporalReferenceFileFormat(types));
    }
}
