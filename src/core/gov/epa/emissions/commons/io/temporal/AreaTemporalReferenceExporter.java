package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class AreaTemporalReferenceExporter extends GenericExporter {
    public AreaTemporalReferenceExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        super(dataset, datasource, new AreaTemporalReferenceFileFormat(types));
    }
}