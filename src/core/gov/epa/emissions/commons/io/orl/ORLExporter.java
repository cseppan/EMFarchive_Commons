package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

public class ORLExporter extends GenericExporter {

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, fileFormat, dataFormatFactory);
        setDelimiter(",");
    }

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat) {
        this(dataset, dbServer, fileFormat, new NonVersionedDataFormatFactory());
    }

}
