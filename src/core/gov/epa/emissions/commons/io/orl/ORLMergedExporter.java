package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;

import java.io.File;

public class ORLMergedExporter implements Exporter {

    private ORLExporter delegate;

    public ORLMergedExporter(Dataset dataset, String rowFilters, DbServer dbServer, Integer optimizedBatchSize) {
        delegate = new ORLExporter(dataset, rowFilters, dbServer, fileFormat(dbServer.getSqlDataTypes()), optimizedBatchSize);
    }

    public ORLMergedExporter(Dataset dataset, String rowFilters, DbServer dbServer,
            DataFormatFactory formatFactory, Integer optimizedBatchSize) {
        delegate = new ORLExporter(dataset, rowFilters, dbServer, fileFormat(dbServer.getSqlDataTypes()), formatFactory, optimizedBatchSize);
    }

    private FileFormat fileFormat(SqlDataTypes sqlDataTypes) {
        return new ORLMergedFileFormat(sqlDataTypes);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }
    
    public long getExportedLinesCount() {
        return delegate.getExportedLinesCount();
    }

}
