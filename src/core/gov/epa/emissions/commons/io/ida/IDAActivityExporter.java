package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class IDAActivityExporter extends GenericExporter {

    public IDAActivityExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat) {
        super(dataset, dbServer, fileFormat);
        setDelimiter(" ");
    }

    public IDAActivityExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, fileFormat, dataFormatFactory);
        setDelimiter(" ");
    }

}
