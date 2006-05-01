package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

public class ORLExporter extends GenericExporter {

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) {
        super(dataset, dbServer, fileFormat, dataFormatFactory, optimizedBatchSize);
        setDelimiter(",");
    }

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, Integer optimizeBatchSize) {
        this(dataset, dbServer, fileFormat, new NonVersionedDataFormatFactory(),optimizeBatchSize);
    }

}
