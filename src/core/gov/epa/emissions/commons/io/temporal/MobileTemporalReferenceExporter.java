package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class MobileTemporalReferenceExporter extends GenericExporter {
    public MobileTemporalReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new MobileTemporalReferenceFileFormat(types));
    }
}