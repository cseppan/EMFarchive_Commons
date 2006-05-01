package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;

import java.io.File;

public class ORLNonRoadExporter implements Exporter {

    private ORLExporter delegate;

    public ORLNonRoadExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizedBatchSize) {
        delegate = new ORLExporter(dataset, dbServer, fileFormat(sqlDataTypes), optimizedBatchSize);
    }

    public ORLNonRoadExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory, Integer optimizedBatchSize) {
        delegate = new ORLExporter(dataset, dbServer, fileFormat(sqlDataTypes), formatFactory, optimizedBatchSize);
    }

    private FileFormat fileFormat(SqlDataTypes sqlDataTypes) {
        return new ORLNonRoadFileFormat(sqlDataTypes);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

}
