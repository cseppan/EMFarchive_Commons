package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class IDAActivityExporter extends GenericExporter {

    public IDAActivityExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, Integer optimizedBatchSize) {
        super(dataset, dbServer, fileFormat, optimizedBatchSize);
        setDelimiter(" ");
    }

    public IDAActivityExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) {
        super(dataset, dbServer, fileFormat, dataFormatFactory, optimizedBatchSize);
        setDelimiter(" ");
    }

}
