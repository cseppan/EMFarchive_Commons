package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class DaySpecPointInventoryExporter extends GenericExporter {
    public DaySpecPointInventoryExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types) {
        super(dataset, dbServer, new DaySpecPointInventoryFileFormat(types));
        super.setDelimiter("");
    }
    
    public DaySpecPointInventoryExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        super(dataset, dbServer, new DaySpecPointInventoryFileFormat(types), factory);
        super.setDelimiter("");
    }
}
