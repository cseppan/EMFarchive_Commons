package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class GridCrossReferenceExporter extends GenericExporter {
    public GridCrossReferenceExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        super(dataset, datasource, new GridCrossRefFileFormat(types));
    }
    
    public GridCrossReferenceExporter(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, datasource, new GridCrossRefFileFormat(types), factory);
    }

}
