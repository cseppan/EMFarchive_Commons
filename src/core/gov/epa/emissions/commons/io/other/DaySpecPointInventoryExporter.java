package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class DaySpecPointInventoryExporter extends GenericExporter {
    public DaySpecPointInventoryExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        super(dataset, datasource, new DaySpecPointInventoryFileFormat(types));
        super.setDelimiter("");
    }
    
    public DaySpecPointInventoryExporter(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, datasource, new DaySpecPointInventoryFileFormat(types), factory);
        super.setDelimiter("");
    }
}
