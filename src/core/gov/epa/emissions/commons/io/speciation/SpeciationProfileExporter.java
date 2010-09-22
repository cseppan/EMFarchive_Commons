package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class SpeciationProfileExporter extends GenericExporter {
    public SpeciationProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, "", dbServer, new SpeciationProfileFileFormat(types), optimizedBatchSize);
    }
    
    public SpeciationProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, "", dbServer, new SpeciationProfileFileFormat(types), factory, optimizedBatchSize);
    }

}
