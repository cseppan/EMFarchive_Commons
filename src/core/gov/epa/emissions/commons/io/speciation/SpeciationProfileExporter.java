package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class SpeciationProfileExporter extends GenericExporter {
    public SpeciationProfileExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        super(dataset, datasource, new SpeciationProfileFileFormat(types));
    }
    
    public SpeciationProfileExporter(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, datasource, new SpeciationProfileFileFormat(types), factory);
    }

}
